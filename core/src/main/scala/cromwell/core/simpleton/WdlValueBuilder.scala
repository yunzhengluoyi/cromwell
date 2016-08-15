package cromwell.core.simpleton

import wdl4s.TaskOutput
import wdl4s.types.{WdlArrayType, WdlMapType, WdlPrimitiveType, WdlType}
import wdl4s.values.{WdlArray, WdlMap, WdlValue}

import scala.collection.Map
import scala.language.postfixOps
import cromwell.core.simpleton.WdlValueSimpleton._


/** Builds arbitrary WdlValues from WdlSimpletons. */
object WdlValueBuilder {

  private val SimpletonPattern = """
        (?x)                             # Turn on comments and whitespace insensitivity.

        ^(                               # Begin capture.

          [a-zA-Z][a-zA-Z0-9_]*          # WDL identifier naming pattern of an initial alpha character followed by zero
                                         # or more alphanumeric or underscore characters.

        )                                # End capture.

        (                                # Begin capture.
          .*                             # Optional simpleton key/component path after a WDL identifier.
        )                                # End capture.

                     """.trim.r          // The trim is necessary as (?x) must be at the beginning of the regex.

  private val ArrayElementPattern = """
        (?x)                             # Turn on comments and whitespace insensitivity.

        ^(                               # Begin capture.  This outer capture grabs all the stuff that will
                                         # be ignored as the simpleton parsing "descends" into the array.

          \[                             # Literal opening square brace.
            (                            # Begin nested capture.
              \d+                        # Digits.
            )                            # End nested capture.
          \]                             # Literal closing square brace.

        )                                # End capture.

        (                                # Begin capture.
          .*                             # Possibly more stuff.
        )                                # End capture.

                  """.trim.r          // The trim is necessary as (?x) must be at the beginning of the regex.

  private val MapElementPattern = """
        (?x)                             # Turn on comments and whitespace insensitivity.

        ^(                               # Begin capture.  This outer capture grabs all the stuff that will
                                         # be ignored as the simpleton parsing "descends" into the map.

          :                              # colon
            (                            # Begin nested capture.
              (?:                        #   Begin nested nested non-capturing group.
                \\\[                     #     An escaped open square brace;
                  |                      #       or
                \\\]                     #     an escaped closing square brace;
                  |                      #       or
                \\:                      #     an escaped colon;
                  |                      #       or
                [^]\[:]                  #     any character other than closing square brace, opening square brace, or colon.
              )                          #   End nested nested non-capturing group.
              +                          #   One or more of the above characters or escaped metacharacters.
            )                            # End nested capture.

        )                                # End capture.

        (                                # Begin capture.
          .*                             # Possibly more stuff.
        )                                # End capture.

                     """.trim.r          // The trim is necessary as (?x) must be at the beginning of the regex.


  private def toWdlValue(outputType: WdlType, components: Traversable[SimpletonComponent]): WdlValue = {

    // Returns a tuple of the index into the outermost array and a `SimpletonComponent` whose path reflects the "descent"
    // into the array.  e.g. for a component
    // SimpletonComponent("[0][1]", v) this would return (0 -> SimpletonComponent("[1]", v)).
    def descendIntoArray(component: SimpletonComponent): (Int, SimpletonComponent) = {
      component.path match { case ArrayElementPattern(_, index, more) => index.toInt -> component.copy(path = more)}
    }

    // Returns a tuple of the key into the outermost map and a `SimpletonComponent` whose path reflects the "descent"
    // into the map.  e.g. for a component
    // SimpletonComponent(":bar:baz", v) this would return ("bar" -> SimpletonComponent(":baz", v)).
    // Map keys are treated as Strings by this method, the caller must ultimately do the appropriate coercion to the
    // actual map key type.
    def descendIntoMap(component: SimpletonComponent): (String, SimpletonComponent) = {
      component.path match { case MapElementPattern(_, key, more) => key.unescapeMeta -> component.copy(path = more)}
    }

    // Group tuples by key using a Map with key type `K`.
    def group[K](tuples: Traversable[(K, SimpletonComponent)]): Map[K, Traversable[SimpletonComponent]] = {
      tuples groupBy { case (i, _) => i } mapValues { _ map { case (i, s) => s} }
    }

    outputType match {
      case _: WdlPrimitiveType => components collectFirst { case SimpletonComponent(_, v) => v } get
      case arrayType: WdlArrayType =>
        val groupedByArrayIndex: Map[Int, Traversable[SimpletonComponent]] = group(components map descendIntoArray)
        WdlArray(arrayType, groupedByArrayIndex.toList.sortBy(_._1) map { case (_, s) => toWdlValue(arrayType.memberType, s) })
      case mapType: WdlMapType =>
        val groupedByMapKey: Map[String, Traversable[SimpletonComponent]] = group(components map descendIntoMap)
        // map keys are guaranteed by the WDL spec to be primitives, so the "coerceRawValue(..).get" is safe.
        WdlMap(mapType, groupedByMapKey map { case (k, ss) => mapType.keyType.coerceRawValue(k).get -> toWdlValue(mapType.valueType, ss) } toMap)
    }
  }

  /** The two fields of the `SimpletonComponent` product type have types identical to those of the `WdlValueSimpleton`
    * product type, but a `SimpletonComponent` is conceptually different from a `WdlValueSimpleton`.
    * `SimpletonComponent` removes the outermost "name" piece of the `WdlValueSimpleton` key, so `path` contains only the
    * path to the element.  e.g. for a `WdlValueSimpleton` of
    *
    * {{{
    * WdlValueSimpleton("foo:bar[0]", WdlString("baz"))
    * }}}
    *
    * the corresponding `SimpletonComponent` would be
    *
    * {{{
    * SimpletonComponent(":bar[0]", WdlString("baz"))
    * }}}
    */
  private case class SimpletonComponent(path: String, value: WdlValue)

  def build(taskOutputs: Traversable[TaskOutput], simpletons: Traversable[WdlValueSimpleton]): Map[String, WdlValue] = {

    def simpletonToComponent(name: String)(simpleton: WdlValueSimpleton): SimpletonComponent = {
      SimpletonComponent(simpleton.simpletonKey.drop(name.length), simpleton.simpletonValue)
    }

    // This is meant to "rehydrate" simpletonized WdlValues back to WdlValues.  It is assumed that these WdlValues were
    // "dehydrated" to WdlValueSimpletons correctly. This code is not robust to corrupt input whatsoever.
    val types = taskOutputs map { o => o.name -> o.wdlType } toMap
    val simpletonsByOutputName = simpletons groupBy { _.simpletonKey match { case SimpletonPattern(i, _) => i } }
    val simpletonComponentsByOutputName = simpletonsByOutputName map { case (name, ss) => name -> (ss map simpletonToComponent(name)) }
    types map { case (name, outputType) => name -> toWdlValue(outputType, simpletonComponentsByOutputName(name))}
  }
}

