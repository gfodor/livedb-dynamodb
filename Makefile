.PHONY: all test clean

COFFEE = node_modules/.bin/coffee
UGLIFY = node_modules/.bin/uglifyjs -d WEB=true

all:

watch:
	$(COFFEE) -c -w -o lib src

clean:
	rm -rf lib/*

test:
	node_modules/.bin/mocha --require coffee-script --compilers coffee:coffee-script --recursive ./test

