package mx.cic

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import java.io._
import java.util.Properties

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */
// Importar librerías necesarios para el procesamiento

object WordCount {
  // Funcion principal main
  def main(args: Array[String]) {

    var salida = args(1)
    var directorio = args(0)
    // Definir y crear una instancia para crear un achico de salida que contendrá el resultado de los procesos
    // Directorio de salida: /home/josman/manuel_castillo/src/main/resources/out/

    val archivo = new File(salida + "/resultados.txt")
    val bw = new BufferedWriter(new FileWriter(archivo))

    // Obtener la lista de los archivos contenidas en el directorio de entrada: /home/josman/manuel_castillo/src/main/resources

    val listArchivos = obtenerListArchivos(args(0))
    var archivos = ""
    for (a <- listArchivos){
      archivos =  archivos + " " + a.toString();
    }

    //Escribir los nombres de los archivos
    bw.write(archivos)
    bw.write("\n" + "**************************************************************************************************"+"\n")

    //Iterar la lista de los archivos para obtener el total de palabras por archivo
    for (e <- listArchivos) {
      var totalPalabras = TotalpalabrasArchivo(e.toString())
      bw.write(totalPalabras + ',')
    }
    bw.write("\n" + "*************************************************************************************************" + "\n")

    val total = Totalpalabras(directorio)
    bw.write("Total de palabras: " + total)

    bw.write("\n" + "*************************************************************************************************" + "\n")

    //Iterar la lista de los archivos para obtener las palabras por archivo
    for (e <- listArchivos) {
      bw.write(e.toString() + "\n\n")
      var Palabras = PalabrasArchivo(e.toString())
      bw.write(Palabras + "\n\n")
    }
    bw.close()
  }

  //Definir la función para obtener las palabras por archivo, estableciendo como parámetro de entrada
  // la ruta del archivo a procesar en el parametro de entrada definido directorio: /home/josman/manuel_castillo/src/main/resources
  def PalabrasArchivo(archivo:String): String = {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    //Leer el archivo de entrada a procesar
    val text = env.readTextFile(archivo) //Leer
    val counts = text.flatMap {_.toLowerCase.split("\\W+")}
      .map {(_, 1)}
      .groupBy(0)
      .sum(1)
    // execute and print result
    return counts.collect().toString()
  }

  //Definir la función para obtener el total de palabras por archivo, estableciendo como parámetro de entrada
  // la ruta del archivo a procesar en el parametro de entrada definido directorio: /home/josman/manuel_castillo/src/main/resources
    def TotalpalabrasArchivo(archivo:String): String = {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    //Leer el archivo de entrada a procesar
    val text = env.readTextFile(archivo) //Leer
    val counts = text.flatMap {_.toLowerCase.split("\\W+")}
      .map {(_, 1)}
      .sum(1)
    // execute and print result
    return counts.collect().toString()
    //counts.print()
  }

  def Totalpalabras(archivo:String): String = {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    //Leer el archivo de entrada a procesar
    val text = env.readTextFile(archivo) //Leer
    val counts = text.flatMap {_.toLowerCase.split("\\W+")}
      .map {(_, 1)}
      .sum(1)
    // execute and print result
    return  return counts.collect().toString
  }

  //Definir la función "obtenerListArchivos" la cual obtiene la lista de archivos contenida en el directorio de entrada.

  def obtenerListArchivos(directorio: String): List[File] = {
    val d = new File(directorio)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    }
    else List[File]()
    }
}

