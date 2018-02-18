package com.stratio.examples.graphframes

import org.apache.spark.sql._
import org.graphframes._


object ExampleGraphFrames {

  @transient lazy val sparkSession: SparkSession =
    SparkSession
      .builder()
      .appName("SparkGraphFrames")
      .config("spark.master", "local")
      .getOrCreate()

  import sparkSession.implicits._


  /**
    * No tiene sentido definir VertexId, VertexProperty y EdgeProperty
    * Un GraphFrame esta formado por dos DataFrames
    *   1. vertices: Datafame
    *   2. edges: DataFrame
    *
    *   Con lo cual son ni mas ni menos Dataset[Row].
    *   Por lo tanto hay que verlos con grupos de Rows, con sus schemas de columns
    *
    *   //type VertexProperty = (String,Int)
    *   //type EdgeProperty = Int
    */



  def main(args:Array[String]) = {
    sparkSession.sparkContext.setLogLevel("WARN")


    /** En graphframe ya no es obligatorio utilizar un VertexId como tipo Long.
      * i.e puedes identificar los vertices como te de la gana.
      * Y tampoco es obligatorio tener separado un VertexId y un VertexProperty,
      * lo que si es obligatorio es que el primer campo del DF de Vertices se identifique como id
      */
    val vertexArray = Array(
      ("1", "Alice", 28),
      ("2", "Bob", 27),
      ("3", "Charlie", 65),
      ("4", "David", 42),
      ("5", "Ed", 55),
      ("6", "Fran", 50)
    )
    /** Observacion: Los 2 primeros valores de la tupla son los identificadores de los vertices.
      * Observar que ya no hay que usar la clase Edge.
      * lo que si es obligatorio es que el DF de edges tenga los campos src y dst como identificadores
      * de los vertices origen y destino respectivamente.
      */
    val edgeArray = Array(
      ("2", "1", 7),
      ("2", "4", 2),
      ("3", "2", 4),
      ("3", "6", 3),
      ("4", "1", 1),
      ("5", "2", 2),
      ("5", "3", 8),
      ("5", "6", 3)
    )

    /**
      * Hasta ahora solo tenemos 2 arrays. Tenemos que convertirlos en 2 DataFrames y juntarlos en un GraphFrame
      */
    println("\n[Carga del grafo]")
    val g = getGraphFrame(vertexArray, edgeArray)

    /**
      * Diferentes formas de filtrar los vertices de un grafo
      *   showGreaterThan --> Se le pasa una edad fija (menos reutilizable)
      *   showFilteredVertex --> Se le pasa una funcion: Int => Boolean (mucho mas reutilizable)
      */
    println("\n[Mayores que 40]")
    //showGreaterThan(g,40)

    val greatherThan40: Int => Boolean = (x: Int) => {x > 40}
    showFilteredVertex(g, greatherThan40)
    // Tambien valdria --> showFilteredVertex(g, (x:Int) => x > 40)

    /**
      * En la ultima version de Graphframes han sacado como atributo triplets. Tan facil como acceder a g.triplets.
      * Nosotros aqui utilizaremos dos opciones mas avanzadas:
      * OPCIONES:
      *   1. showJoinTriplets --> Utilizar joins entre vertices y aristas (no utilizas la potencia de GraphFrames)
      *   2. showTripletsDSL --> Utilizando Motif Finding, una DSL de grafos (muy potente)
      */
    println("\n[Mostrar Triplets]")
    //showJoinTriplets(g)
    showTripletsDSL(g)

    /**
      * Al no tener VertexProperty, sea cual sea, no tiene sentido transformarlo a case classes.
      * Hay que ver un GraphFrame como 2 Dataset[Row], con sus schemas. --> "2 TABLAS"
      * Observacion: No son Dataset[T] sino Dataset[Row], i.e DataFrames
      */
    //println("\n[Transformar grafo]")
    //val userGraph = getUserGraph(g)

    /**
      * Uno de los algoritmos mas utiles y usados es el pageRank --> Importancia de la pagina/vertice dentro de la red.
      */
    println("\n[PageRank de users]")
    pageRankUserGraph(g)

  }

  /**
    * Recibimos 2 arrays: uno de vertices y otro de aristas:
    *   vertexArray: Array[(String,String,Int)], edgeArray: Array[(String,String,Int)]
    * los cuales aun los tenemos en memoria local. Tenemos que generar sus Dataframes paralelizados para mandarlos al cluster.
    * OBS: Tener en cuenta que los Dataframes deben tener algunas columnas de nombre fijo: "id", "src" y "dst".
    * Por convencion utilizaremos los nombres de columnas no fijos de la siguiente forma:
    *   "name" --> Nombre del user
    *   "age" --> Edad del user
    *   "relationship" --> numero de likes
    *
    * Despues de generarlos los mostraremos utilizando el metodo show de Dataframe.
    *
    * Por ultimo crearemos el grafo partiendo de ambos Dataframes.
    *
    * IMPORTANTE: Como lo vamos a utilizar bastante, deberiamos cachear el grafoframe.
    *
    * @param vertexArray Array de vertices, cada vertice se define por una tupla (String,String,Int)
    * @param edgeArray Array de aristas, cada arista se define por una tupla (String,String,Int)
    * @return un grafo compuesto por los vertices y aristas que toma como parametro.
    */
  def getGraphFrame(vertexArray: Array[(String,String,Int)], edgeArray: Array[(String,String,Int)]):
    GraphFrame = {

    val vertex = sparkSession.createDataFrame(vertexArray).toDF("id", "name", "age")
    vertex.show(10,true)

    val edge = sparkSession.createDataFrame(edgeArray).toDF("src", "dst", "relationship")
    edge.show(10,true)

    val graphFrame: GraphFrame = GraphFrame(vertex, edge)

    graphFrame.cache
  }

  /**
    * Recibimos el grafo creado y vamos a quedarnos con los vertices que sean estrictamente mayores de 'age' (argumento de la funcion)
    * Para ello utilizaremos la funcion filter de dataframe pasandole la condicion a filtrar (columna age > param age)
    *
    * Posteriormente mostraremos los resultados del filtrado
    *
    * @param g Grafo a filtrar
    * @param age Edad para condicion de filtrado de los vertices
    */
  def showGreaterThan(g: GraphFrame, age: Int) = {
    g.vertices.filter($"age" > age).collect().foreach{
      case row => println(s"${row.getAs[Int]("name")} tiene ${row.getAs[Int]("age")} años")
    }
  }

  /**
    * Recibimos el grafo creado y vamos a quedarnos con los vertices que cumplan una condicion de filtrado (argumento de la funcion)
    * Para ello utilizaremos la funcion filter de dataframe indicandole como debe filtrar (utilizando la funcion de filtrado)
    *
    * Posteriormente mostraremos los resultados del filtrado
    *
    * @param g Grafo a filtrar
    * @param fFilter funcion de filtrado de los vertices
    */
  def showFilteredVertex(g: GraphFrame, fFilter: Int => Boolean) = {
    g.vertices.filter(row => fFilter(row.getAs[Int]("age"))).collect().foreach{
      case row => println(s"${row.getAs[Int]("name")} tiene ${row.getAs[Int]("age")} años")
    }
  }

  /**
    * Recibimos el grafo creado y queremos mostrar los triplets del grafo (via join)
    * Para ello tenemos que hacer 2 joins:
    *   result = egdes JOIN vertices (para sacar el src junto con el edge)
    *   result JOIN vertices (para añadir el dst)
    * OBS: Las columnas name y age se duplicaran, por lo tanto es necesario renombrarlas con .as("")
    *
    * Posteriormente mostraremos los resultados de los triplets
    *
    * @param g Grafo sobre el cual se muestran sus triplets (via join)
    */
  def showJoinTriplets(g: GraphFrame): Unit = {
    val tripletsOnlySrc: DataFrame = g.edges.join(g.vertices, $"src" === $"id")
        .select(
          $"src",
          $"dst",
          $"relationship",
          $"name".as("name_src"),
          $"age".as("age_src"))
    val triplets: DataFrame = tripletsOnlySrc.join(g.vertices, $"dst" === $"id")
      .select(
        $"src",
        $"dst",
        $"relationship",
        $"name_src",
        $"age_src",
        $"name".as("name_dst"),
        $"age".as("age_dst"))

    triplets.collect().foreach{
      case row =>
        println(s"${row.getAs[String]("name_src")} ha mandado ${row.getAs[Int]("relationship")} " +
          s"likes a ${row.getAs[String]("name_dst")}")
    }
  }

  /**
    * Recibimos el grafo creado y queremos mostrar los triplets del grafo (via DSL)
    * Para ello tenemos que utilizar el metodo find de GraphFrame pasandole como argumento una consulta "grafal"
    * Y posteriormente seleccionaremos solo los siguientes campos:
    *   1. Nombre del origen --> Lo llamaremos src_name
    *   2. Nombre del destino --> Lo llamaremos dst_name
    *   3. Relacion entre ellos (numero de likes) --> Lo llamaremos likes
    *
    * Posteriormente mostraremos los resultados de los triplets
    *
    * @param g Grafo sobre el cual se muestran sus triplets (via join)
    */
  def showTripletsDSL(g: GraphFrame): Unit = {
    val triplets: DataFrame = g.find("(a)-[e]->(b)")
      .select($"a.name".as("src_name"), $"b.name".as("dst_name"), $"e.relationship".as("likes"))

    triplets.collect().foreach{
      case row =>
        println(s"${row.getAs[String]("src_name")} ha mandado ${row.getAs[Int]("likes")} " +
          s"likes a ${row.getAs[String]("dst_name")}")
    }
  }



  /**
    * Vamos a utilizar el metodo pageRank de la clase GraphFrame: g.pageRank.run()
    * En nuestro caso indicaremos dos parametros
    *   1. Maximo de iteraciones del algoritmo --> g.pageRank.maxIter(3)
    *   2. Probabilidad de reset o valor alpha, resetProb = 0.15 --> g.pageRank.maxIter(3).resetProbability(0.15)
    *
    * Observacion: tras aplicar las variables hay que llamar explicitamente al metodo run()
    * Observacion 2: Nos quedaremos solo con el pageRank de los vertices, aunque tambien se computa el de las aristas
    *
    * @param g grafo sobre el que queremos mostrar su pageRank de vertices
    */
  def pageRankUserGraph(g: GraphFrame): Unit = {
    val rank: DataFrame = g.pageRank.maxIter(3).resetProbability(0.15).run().vertices
    // Y lo bueno es que no es necesario hacer join, el resultado es un Dataframe con una nueva columna pagerank
    rank.select($"pagerank",$"name").sort(-$"pagerank").show(10,true)
  }
}
