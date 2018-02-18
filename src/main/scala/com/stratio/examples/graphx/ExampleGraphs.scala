package com.stratio.examples.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object ExampleGraphs {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("SparkGraph")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  // type VertexId = Long --> ya se define globalmente en el package org.apache.spark.graphx
  type VertexProperty = (String,Int)
  type EdgeProperty = Int

  def main(args:Array[String]) = {
    sc.setLogLevel("WARN")

    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )

    /**
      * Observacion: Aunque tu construyes objetos Edge con 3 parametros
      * el tipo siempre es Edge[T], siendo T el tipo de la arista, en nuestro caso T = Int
      * los 2 primeros son los VertexId
      */
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )

    /**
      * Hasta ahora solo tenemos 2 arrays. Tenemos que convertirlos en 2 RDDs parallelizados
      */
    println("\n[Carga del grafo]")
    val g = getGraph(vertexArray, edgeArray)

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
      * La forma mas eficiente de unir Edges con cada Vertex
      */
    println("\n[Mostrar Triplets]")
    showTriplets(g)

    /**
      * En vez de tener un VertexProperty basado en una tupla es preferible utilizar una case class, te da mas opciones
      * y mas claridad al codigo.
      */
    println("\n[Transformar grafo]")
    val userGraph = getUserGraph(g)

    /**
      * Uno de los algoritmos mas utiles y usados es el pageRank --> Importancia de la pagina/vertice dentro de la red.
      */
    println("\n[PageRank de users]")
    pageRankUserGraph(userGraph)
  }

  /**
    * Recibiendo dos arrays: vertexArray: Array[(VertexId,VertexProperty)], edgeArray: Array[Edge[EdgeProperty]]
    * se construye un Graph[VertexProperty,EdgeProperty], partiendo de los RDD de vertices y edges convertidos
    *
    * @param vertexArray Array de vertices, cada vertice de tipo 'VertexProperty'
    * @param edgeArray Array de aristas, cada arista de tipo 'EdgeProperty'
    * @return un grafo compuesto por los vertices y aristas que toma como parametro.
    */
  def getGraph(vertexArray: Array[(VertexId,VertexProperty)], edgeArray: Array[Edge[EdgeProperty]]):
    Graph[VertexProperty,EdgeProperty] = {

    val vertexRDD: RDD[(VertexId, VertexProperty)] = sc.parallelize(vertexArray)
    vertexRDD.collect().foreach{
      case (id,(name,edad)) => println(s"$name tiene $edad años")
    }
    val edgeRDD: RDD[Edge[EdgeProperty]] = sc.parallelize(edgeArray)
    edgeRDD.collect().foreach{
      case Edge(vId1, vId2, numlikes) => println(s"vertex $vId1 tiene $numlikes likes a vertex $vId2")
    }
    val graph: Graph[VertexProperty,EdgeProperty] = Graph(vertexRDD,edgeRDD)

    graph.cache
  }

  /**
    * Recibimos el grafo creado y vamos a quedarnos con los vertices que sean
    * estrictamente mayores de 'age' (argumento de la funcion)
    *
    * @param g grafo de tipo Graph[VertexProperty, EdgeProperty]
    * @param age Edad para condicion de filtrado de los vertices
    */
  def showGreaterThan(g: Graph[VertexProperty, EdgeProperty], age: Int) = {
    g.vertices.filter(_._2._2 > age).collect().foreach{
      case v => println(s"${v._2._1} tiene ${v._2._2} años")
    }
  }

  /**
    * Recibimos el grafo creado y vamos a quedarnos con los vertices que
    * cumplan una condicion de filtrado (argumento de la funcion)
    *
    * @param g grafo de tipo Graph[VertexProperty, EdgeProperty]
    * @param fFilter funcion de filtrado de los vertices
    */
  def showFilteredVertex(g: Graph[VertexProperty, EdgeProperty], fFilter: Int => Boolean) = {
    g.vertices.filter(v => fFilter(v._2._2)).collect().foreach{
      case v => println(s"${v._2._1} tiene ${v._2._2} años")
    }
  }

  /**
    * Triplet esta formado por los siguientes campos:
    *   triplet.srcAttr: Vertex origen // triplet.srcAttr._1 is el nombre y triplet.srcAttr._2 es la edad
    *   triplet.dstAttr: Vertex destino
    *   triplet.attr: Edge
    *   triplet.srcId: VertexId del origen
    *   triplet.dstId: VertexId del destino
    *
    * Lo que haremos en este metodo es recoger los triplets del grafo de entrada, recorrerlos e ir mostrando una salida como esta:
    *   --> <Pepito> ha mandado <5> likes a <Juanito>
    *
    * @param g grafo de tipo Graph[VertexProperty, EdgeProperty]
    */
  def showTriplets(g: Graph[VertexProperty, EdgeProperty]): Unit ={
    g.triplets.collect().foreach{
      case triplet => println(s"${triplet.srcAttr._1} ha mandado ${triplet.attr} likes a ${triplet.dstAttr._1}")
    }
  }

  /* Clase User que define un vertice de una forma mas apropiada */
  case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

  /**
    * Se transforma un Graph[VertexProperty, EdgeProperty] a un Graph[User, EdgeProperty] utilizando la funcion mapVertices
    *
    * @param g Grafo cuyos Vertices son de tipo VertexProperty y sus Aristas de tipo EdgeProperty
    * @return El mismo grafo, pero ahora sus vertices seran de tipo UserProperty.
    *         Donde el campo name y age seran exactamente los mismos valores, y enriqueceremos con los campos
    *         inDeg y outDeg que corresponderan a los grados de entrada y salida de cada vertice.
    */
  def getUserGraph(g: Graph[VertexProperty, EdgeProperty]): Graph[User,EdgeProperty] = {
    val initialUserGraph: Graph[User, EdgeProperty] = g.mapVertices{case (id,(name,age)) => User(name,age, 0, 0)}
    val userGraph: Graph[User, EdgeProperty] =
      initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees){
        case (id, u, inDegree) => User(u.name,u.age,inDegree.getOrElse(0),u.outDeg)
      }.outerJoinVertices(initialUserGraph.outDegrees){
        case (id, u, outDegree) => User(u.name,u.age,u.inDeg, outDegree.getOrElse(0))
      }
    userGraph.vertices.collect().foreach{
      case (id, User(name,age, inDeg, outDeg)) => println(s"User $id - $name es seguida por $inDeg personas y sigue a $outDeg personas")
    }
    userGraph.cache
  }

  /**
    * Ejecuta el algoritmo pageRank de los vertices de un grafo de tipo Graph[User, EdgeProperty],
    * y muestra el resultado.
    * Para el algoritmo se utilizara el parametro de tolerancia a 0.01 y el valor de resetProb por defecto (0.15)
    *
    * @param g grafo sobre el que queremos mostrar su pageRank de vertices
    */
  def pageRankUserGraph(g: Graph[User, EdgeProperty]): Unit = {
    val rank: VertexRDD[Double] = g.pageRank(0.01).vertices
    // No voy a utilizar g.joinVertices(rank) porque ello me obligaria a devolver otro grafo.
    // Me vale con hacer un join de RDDs.
    val userByRank: RDD[(User,Double)] = g.vertices.join(rank).map{ case (id,(user,rank)) => (user, rank)}
    userByRank.collect().sortBy(_._2).reverse.foreach{
      case (user,rank) => println(s"$rank - ${user.name}")
    }
  }
}
