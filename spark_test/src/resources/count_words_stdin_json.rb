#!/usr/bin/ruby
#Encoding UTF-8
require 'json'
require '/home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/dependency'

def count_words(json_input)
  #json_input = eval(json_input)
  doc = JSON.load(json_input)
  text = doc["text"]
  parts = text.split(" ")
  doc["count"] = parts.length
  return JSON.dump(doc)
end

if __FILE__ == $0
  hello
  for line in ARGF.each
    log = open("/home/cnavarro/workspace/mixedemotions/me_extractors/spark_test/src/resources/count_words.log","a")
    log.write("Input: #{line}")
    puts(count_words(line))
  end
end   
   
