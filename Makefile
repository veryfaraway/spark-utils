GRADLE_VERSION=5.6.1

build: clean
	gradle build

publish: build
	#gradle publish
	gradle bintrayUpload

local: build
	gradle publishToMavenLocal

clean:
	gradle clean

wrapper:
	gradle wrapper --gradle-version ${GRADLE_VERSION} --distribution-type all
