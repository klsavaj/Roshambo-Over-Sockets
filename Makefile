CC = gcc
CFLAGS = -Wall -Wextra

all: spock

spock: spock.c
	$(CC) $(CFLAGS) -o spock spock.c

clean:
	rm -f spock