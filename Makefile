build: clean
	gradle build

publish: build
	#gradle publish
	gradle bintrayUpload

local: build
	gradle publishToMavenLocal

clean:
	gradle clean
