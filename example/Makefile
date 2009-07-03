# Makefile for sample programs of Hyper Estraier



#================================================================
# Setting Variables
#================================================================


# Generic settings
SHELL = /bin/sh

# Targets
MYBINS = tcutilex tchdbex tcbdbex tcfdbex tctdbex tcadbex tctchat.cgi

# Building binaries
CC = gcc
CFLAGS = -ansi -Wall -pedantic -I. -I..
C99FLAGS = -std=c99 -Wall -pedantic -I. -I..
LDFLAGS =
LIBS = -L. -L.. -ltokyocabinet -lz -lpthread -lm -lc
LDENV = LD_RUN_PATH=/lib:/usr/lib:$(HOME)/lib:/usr/local/lib:.:..



#================================================================
# Suffix rules
#================================================================


.SUFFIXES :
.SUFFIXES : .c .o

.c.o :
	$(CC) -c $(CFLAGS) $<



#================================================================
# Actions
#================================================================


all : $(MYBINS)


clean :
	rm -rf $(MYBINS) *.exe *.o a.out check.out gmon.out leak.log casket* *.tct *.idx.* *~


.PHONY : all clean



#================================================================
# Building binaries
#================================================================


tcutilex : tcutilex.o
	$(LDENV) $(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


tchdbex : tchdbex.o
	$(LDENV) $(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


tcbdbex : tcbdbex.o
	$(LDENV) $(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


tcfdbex : tcfdbex.o
	$(LDENV) $(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


tctdbex : tctdbex.o
	$(LDENV) $(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


tcadbex : tcadbex.o
	$(LDENV) $(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)


tctchat.cgi : tctchat.c
	$(LDENV) $(CC) $(C99FLAGS) -o $@ $< $(LDFLAGS) $(LIBS)



# END OF FILE
