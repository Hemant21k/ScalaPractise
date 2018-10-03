package com.scala.practise

import net.jcazevedo.moultingyaml._
//import CustomYamlProtocol._

object ParserYaml {
/*  https://github.com/jcazevedo/moultingyaml
  http://blog.baita.pro/scala/yaml/parsing/2017/02/20/yaml-parsing-with-snakeyaml-moultingyaml.html
*/
val teamYAML = """---
name: amazing team
scrum:
  name: John
  age: 34
  skills:
  - project management
  - problem solving
developers:
- name: Bob
  age: 26
  skills:
  - java
  - scala
- name: Alice
  age: 24
  skills:
  - scala
  - js
- name: Charlie
  age: 22
  skills: []
..."""

case class Person(name: String, age: Int, skills: Seq[String])

case class Team(name: String, scrum: Person, developers: List[Person])

object CustomYamlProtocol extends DefaultYamlProtocol {
  implicit val personFormat = yamlFormat3(Person)
  implicit val teamFormat = yamlFormat3(Team)
}

import CustomYamlProtocol._

val team = teamYAML.parseYaml.convertTo[Team]

def printYaml()={
  val developerss = team.developers
  println(developerss)
}

def main(args:Array[String])={
  printYaml()
}

}