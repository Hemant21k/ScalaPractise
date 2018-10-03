package com.scala.practise

object YAMLDATA {
  val stmyaml = """---
[CONTROL_AREA]: 
 SIGNATURE: TXT4C
 ALIAS: NVYTP
 OBJECTIVENAME: objective_name
 EDITIONDATE: edition_date
 ERRORCOUNT: error_count
 ERRORTABLE: error_table
 EDITION: edition'
 EDITIONTIME: edition_time
 INPUT_AREAS: INPUT_AREA1,INPUT_AREA2

[INPUT_AREAS]:
 -[INPUT_AREA1]: 
  encrypted_pin: PINS
  Prodcd: PRODUCT_CODE
  Respind: RESPONSE_IND
 -[INPUT_AREA2]: 
  encrypted_pin2: PINS2
  Prodcd2: PRODUCT_CODE2
  Respind2: RESPONSE_IND2
..."""
}