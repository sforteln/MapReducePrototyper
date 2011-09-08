package mapreduce.prototype

import java.io.File
import java.io.PrintWriter
import java.util.List



class MapReducePrototyper {
	def intermediateValues = new ArrayList<Intermediate>()
	
	void input(List inputIn) {
		def counter=intermediateValues.size()
		inputIn.each { line -> 
			def inter = new Intermediate( counter,line)		
			intermediateValues.add(inter)
			counter +=1 
		}
	}
	void input(File inputIn) {
		def counter=intermediateValues.size()
		inputIn.each { it ->
			intermediateValues.add(new Intermediate( counter, it))
			counter +=1 
		}
	}
	
	void map(Closure map) {
		def writer = new Collector()
		intermediateValues.each { it -> 
			map(it.key, it.value, writer)
		}	
		intermediateValues =writer.getValues()
	}
	
	void reduce(Closure reduce){
		//first merge all values with the same key
		def merged = [:]
		intermediateValues.each{
			merged.get(it.key,[]).add(it.value)
		}
		//reset intermediateValues so if can get the new vals
		def writer = new Collector()
		merged.each { key,value ->
			reduce(key, value, writer)
		}
		intermediateValues =writer.getValues()
	}
	
	void printIntermediateValues(PrintWriter pw){
		intermediateValues.each { it -> 
			pw.println(it.toString()) 
		}
	}
	
	def getIntermediateValues(){
		return intermediateValues
	}
	
	def getValuesAsMap(){
		def result = [:]
		intermediateValues.each{
			result[it.key] = it.value 
		}
		return result
	}
	
	class Intermediate{
		Object key, value
		Intermediate(Object keyIn, Object valueIn){
			key=keyIn
			value=valueIn
		}	
		@Override
		String toString(){
			return "${key.toString()} -> ${value.toString()}"
		}
	}
	
	class Collector{ 
		private intermediateValues = new ArrayList<Intermediate>()
		
		def getValues = {
			return intermediateValues
		}
		
		def write  = { key,val ->
			intermediateValues.add(new Intermediate( key, val))
		}
		
	}
}
