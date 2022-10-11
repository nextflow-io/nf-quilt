
config ?= compileClasspath

ifdef module
mm = :${module}:
else
mm =
endif

clean:
	./gradlew clean

compile:
	./gradlew compileGroovy
	./gradlew exportClasspath
	@echo "DONE `date`"

check:
	./gradlew check

check3: compile
	clear
	./gradlew check || open file:///Users/quilt/Documents/GitHub/nf-quilt/plugins/nf-quilt3/build/reports/tests/test/index.html

run:
	./launch.sh run nextflow-io/hello -plugins nf-quilt,nf-quilt3

test-pub:
	mkdir -p /tmp/nf-quilt
	./launch.sh run test/publish.nf
	ls -lR /tmp/nf-quilt

test-quilt: compile
	./launch.sh run test/quilt.nf

test-local:
	./launch.sh run test/quilt.nf --src ~/Downloads/Packages/ernie_igv_demo_ecdcd41/ --pub ~/Downloads/Packages/sprint_2022-09-12

qtest: compile
	clear
	./launch.sh run test/publish.nf --pubdir quilt3://quilt-ernest-staging/nf-quilt/test-out/

shell:
	./gradlew -q --no-daemon --console=plain --init-script gradle/groovysh-init.gradle groovysh
# https://gist.github.com/sandipchitale/6f53c646ec00752d41cddcca243d76cf

#
# Show dependencies try `make deps config=runtime`, `make deps config=google`
#
deps:
	./gradlew -q ${mm}dependencies --configuration ${config}

deps-all:
	./gradlew -q dependencyInsight --configuration ${config} --dependency ${module}

#
# Refresh SNAPSHOTs dependencies
#
refresh:
	./gradlew --refresh-dependencies

#
# Run all tests or selected ones
#
test:
ifndef class
	./gradlew ${mm}test
else
	./gradlew ${mm}test --tests ${class}
endif

#
# Upload JAR artifacts to Maven Central
#
upload:
	./gradlew upload

upload-plugins:
	./gradlew plugins:upload

publish-index:
	./gradlew plugins:publishIndex
