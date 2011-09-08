package mapreduce.prototype

import groovy.util.GroovyTestCase;

class MapReducePrototyperTest extends GroovyTestCase {

	static def simpleInput = [	"Hadoop consists of the Hadoop Common, which provides access to the filesystems supported by Hadoop",
			"The Hadoop Common package contains the necessary JAR files and scripts needed to start Hadoop",
			"The package also provides source code, documentation, and a contribution section which includes projects from the Hadoop Community"]
	
	void testWordCount(){
		def mrPrototype = new MapReducePrototyper()
		//load in string array
		mrPrototype.input(simpleInput)
		//create mapper closure
		def wordCountMapper = { key,val,collector -> 
			val.split(/ /).each {
				collector.write(it,1)
			}
		}
		//apply mapper to current data
		mrPrototype.map(wordCountMapper)
		//make sure the words were split and counted correctly by the mapper
		//instances of the inner class Intermediate
		def mapperResult = mrPrototype.getIntermediateValues()
		def hadoopWordsInMapdata = mapperResult.findAll { word -> word.key.equals('Hadoop') }
		assertEquals(6,hadoopWordsInMapdata.size())
		//create the reducer closure
		def wordCountReducer = { key,vals,collector ->
			collector.write(key,vals.size())
		}
		mrPrototype.reduce(wordCountReducer)
		//get the reducer results as instances of the inner class Intermediate
		def reducerResult = mrPrototype.getIntermediateValues()
		def HadoopCountFromReducer = reducerResult.find{word -> word.key.equals('Hadoop')  }
		assertEquals(6,HadoopCountFromReducer.value)
		//get result as a map and check it
		def resultAsMap = mrPrototype.getValuesAsMap();
		assertTrue(resultAsMap.containsKey('Hadoop'))
		assertEquals(6,resultAsMap['Hadoop'])
		
	}
}