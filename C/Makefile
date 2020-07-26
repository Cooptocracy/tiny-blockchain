objects = client.o block.o block_verify.o

subsystem:
	$(MAKE)	-C sha256

src:$(objects)
	cc -o src:$(objects)

block.o: block.h
block_verify.o: block_verify.h

.PHONY:clean

clean:
	-rm src $(objects)
	make clean 
