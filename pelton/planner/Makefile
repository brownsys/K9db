test/DataFlowGraphLibrary.java: test/DataFlowGraphLibraryConfig.java
	java -jar javacpp.jar test/DataFlowGraphLibraryConfig.java

test/libjniDataFlowGraphLibrary.so: test/DataFlowGraphLibrary.java
	java -jar javacpp.jar test/DataFlowGraphLibrary.java -d test

Main.class: Main.java test/DataFlowGraphLibrary.java test/DataFlowGraphLibraryConfig.java
	javac -cp javacpp.jar:. Main.java

myjar.jar: libgenerator.so test/libjniDataFlowGraphLibrary.so Main.class javacpp.jar
	rm -rf tmp
	mkdir tmp
	cp javacpp.jar tmp/
	cd tmp && jar xf javacpp.jar && rm javacpp.jar
	jar -cfve myjar.jar Main Main.class *.so test -C tmp .
	rm -rf tmp

.PHONY: clean
clean:
	rm -rf Main.class myjar.jar test/*.so test/*.class test/DataFlowGraphLibrary.java tmp
