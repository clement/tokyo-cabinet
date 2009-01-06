/*************************************************************************************************
 * The test cases of the abstract database API
 *                                                      Copyright (C) 2006-2009 Mikio Hirabayashi
 * This file is part of Tokyo Cabinet.
 * Tokyo Cabinet is free software; you can redistribute it and/or modify it under the terms of
 * the GNU Lesser General Public License as published by the Free Software Foundation; either
 * version 2.1 of the License or any later version.  Tokyo Cabinet is distributed in the hope
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * You should have received a copy of the GNU Lesser General Public License along with Tokyo
 * Cabinet; if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330,
 * Boston, MA 02111-1307 USA.
 *************************************************************************************************/


#include <tcutil.h>
#include <tcadb.h>
#include "myconf.h"

#define RECBUFSIZ      32                // buffer for records


/* global variables */
const char *g_progname;                  // program name


/* function prototypes */
int main(int argc, char **argv);
static void usage(void);
static void iprintf(const char *format, ...);
static void iputchar(int c);
static void eprint(TCADB *adb, const char *func);
static bool iterfunc(const void *kbuf, int ksiz, const void *vbuf, int vsiz, void *op);
static int myrand(int range);
static int runwrite(int argc, char **argv);
static int runread(int argc, char **argv);
static int runremove(int argc, char **argv);
static int runrcat(int argc, char **argv);
static int runmisc(int argc, char **argv);
static int runwicked(int argc, char **argv);
static int runcompare(int argc, char **argv);
static int procwrite(const char *name, int rnum);
static int procread(const char *name);
static int procremove(const char *name);
static int procrcat(const char *name, int rnum);
static int procmisc(const char *name, int rnum);
static int procwicked(const char *name, int rnum);
static int proccompare(const char *name, int tnum, int rnum);


/* main routine */
int main(int argc, char **argv){
  g_progname = argv[0];
  srand((unsigned int)(tctime() * 1000) % UINT_MAX);
  if(argc < 2) usage();
  int rv = 0;
  if(!strcmp(argv[1], "write")){
    rv = runwrite(argc, argv);
  } else if(!strcmp(argv[1], "read")){
    rv = runread(argc, argv);
  } else if(!strcmp(argv[1], "remove")){
    rv = runremove(argc, argv);
  } else if(!strcmp(argv[1], "rcat")){
    rv = runrcat(argc, argv);
  } else if(!strcmp(argv[1], "misc")){
    rv = runmisc(argc, argv);
  } else if(!strcmp(argv[1], "wicked")){
    rv = runwicked(argc, argv);
  } else if(!strcmp(argv[1], "compare")){
    rv = runcompare(argc, argv);
  } else {
    usage();
  }
  return rv;
}


/* print the usage and exit */
static void usage(void){
  fprintf(stderr, "%s: test cases of the abstract database API of Tokyo Cabinet\n", g_progname);
  fprintf(stderr, "\n");
  fprintf(stderr, "usage:\n");
  fprintf(stderr, "  %s write name rnum\n", g_progname);
  fprintf(stderr, "  %s read name\n", g_progname);
  fprintf(stderr, "  %s remove name\n", g_progname);
  fprintf(stderr, "  %s rcat name rnum\n", g_progname);
  fprintf(stderr, "  %s misc name rnum\n", g_progname);
  fprintf(stderr, "  %s wicked name rnum\n", g_progname);
  fprintf(stderr, "  %s compare name tnum rnum\n", g_progname);
  fprintf(stderr, "\n");
  exit(1);
}


/* print formatted information string and flush the buffer */
static void iprintf(const char *format, ...){
  va_list ap;
  va_start(ap, format);
  vprintf(format, ap);
  fflush(stdout);
  va_end(ap);
}


/* print a character and flush the buffer */
static void iputchar(int c){
  putchar(c);
  fflush(stdout);
}


/* print error message of abstract database */
static void eprint(TCADB *adb, const char *func){
  fprintf(stderr, "%s: %s: error\n", g_progname, func);
}


/* iterator function */
static bool iterfunc(const void *kbuf, int ksiz, const void *vbuf, int vsiz, void *op){
  unsigned int sum = 0;
  while(--ksiz >= 0){
    sum += ((char *)kbuf)[ksiz];
  }
  while(--vsiz >= 0){
    sum += ((char *)vbuf)[vsiz];
  }
  return myrand(100 + (sum & 0xff)) > 0;
}


/* get a random number */
static int myrand(int range){
  return (int)((double)range * rand() / (RAND_MAX + 1.0));
}


/* parse arguments of write command */
static int runwrite(int argc, char **argv){
  char *name = NULL;
  char *rstr = NULL;
  for(int i = 2; i < argc; i++){
    if(!name && argv[i][0] == '-'){
      usage();
    } else if(!name){
      name = argv[i];
    } else if(!rstr){
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if(!name || !rstr) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int rv = procwrite(name, rnum);
  return rv;
}


/* parse arguments of read command */
static int runread(int argc, char **argv){
  char *name = NULL;
  for(int i = 2; i < argc; i++){
    if(!name && argv[i][0] == '-'){
      usage();
    } else if(!name){
      name = argv[i];
    } else {
      usage();
    }
  }
  if(!name) usage();
  int rv = procread(name);
  return rv;
}


/* parse arguments of remove command */
static int runremove(int argc, char **argv){
  char *name = NULL;
  for(int i = 2; i < argc; i++){
    if(!name && argv[i][0] == '-'){
      usage();
    } else if(!name){
      name = argv[i];
    } else {
      usage();
    }
  }
  if(!name) usage();
  int rv = procremove(name);
  return rv;
}


/* parse arguments of rcat command */
static int runrcat(int argc, char **argv){
  char *name = NULL;
  char *rstr = NULL;
  for(int i = 2; i < argc; i++){
    if(!name && argv[i][0] == '-'){
      usage();
    } else if(!name){
      name = argv[i];
    } else if(!rstr){
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if(!name || !rstr) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int rv = procrcat(name, rnum);
  return rv;
}


/* parse arguments of misc command */
static int runmisc(int argc, char **argv){
  char *name = NULL;
  char *rstr = NULL;
  for(int i = 2; i < argc; i++){
    if(!name && argv[i][0] == '-'){
      usage();
    } else if(!name){
      name = argv[i];
    } else if(!rstr){
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if(!name || !rstr) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int rv = procmisc(name, rnum);
  return rv;
}


/* parse arguments of wicked command */
static int runwicked(int argc, char **argv){
  char *name = NULL;
  char *rstr = NULL;
  for(int i = 2; i < argc; i++){
    if(!name && argv[i][0] == '-'){
      usage();
    } else if(!name){
      name = argv[i];
    } else if(!rstr){
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if(!name || !rstr) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int rv = procwicked(name, rnum);
  return rv;
}


/* parse arguments of compare command */
static int runcompare(int argc, char **argv){
  char *name = NULL;
  char *tstr = NULL;
  char *rstr = NULL;
  for(int i = 2; i < argc; i++){
    if(!name && argv[i][0] == '-'){
      usage();
    } else if(!name){
      name = argv[i];
    } else if(!tstr){
      tstr = argv[i];
    } else if(!rstr){
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if(!name || !tstr || !rstr) usage();
  int tnum = tcatoi(tstr);
  int rnum = tcatoi(rstr);
  if(tnum < 1 || rnum < 1) usage();
  int rv = proccompare(name, tnum, rnum);
  return rv;
}


/* perform write command */
static int procwrite(const char *name, int rnum){
  iprintf("<Writing Test>\n  name=%s  rnum=%d\n\n", name, rnum);
  bool err = false;
  double stime = tctime();
  TCADB *adb = tcadbnew();
  if(!tcadbopen(adb, name)){
    eprint(adb, "tcadbopen");
    err = true;
  }
  for(int i = 1; i <= rnum; i++){
    char buf[RECBUFSIZ];
    int len = sprintf(buf, "%08d", i);
    if(!tcadbput(adb, buf, len, buf, len)){
      eprint(adb, "tcadbput");
      err = true;
      break;
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tcadbrnum(adb));
  iprintf("size: %llu\n", (unsigned long long)tcadbsize(adb));
  if(!tcadbclose(adb)){
    eprint(adb, "tcadbclose");
    err = true;
  }
  tcadbdel(adb);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform read command */
static int procread(const char *name){
  iprintf("<Reading Test>\n  name=%s\n\n", name);
  bool err = false;
  double stime = tctime();
  TCADB *adb = tcadbnew();
  if(!tcadbopen(adb, name)){
    eprint(adb, "tcadbopen");
    err = true;
  }
  int rnum = tcadbrnum(adb);
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%08d", i);
    int vsiz;
    char *vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
    if(!vbuf){
      eprint(adb, "tcadbget");
      err = true;
      break;
    }
    tcfree(vbuf);
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tcadbrnum(adb));
  iprintf("size: %llu\n", (unsigned long long)tcadbsize(adb));
  if(!tcadbclose(adb)){
    eprint(adb, "tcadbclose");
    err = true;
  }
  tcadbdel(adb);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform remove command */
static int procremove(const char *name){
  iprintf("<Removing Test>\n  name=%s\n\n", name);
  bool err = false;
  double stime = tctime();
  TCADB *adb = tcadbnew();
  if(!tcadbopen(adb, name)){
    eprint(adb, "tcadbopen");
    err = true;
  }
  int rnum = tcadbrnum(adb);
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%08d", i);
    if(!tcadbout(adb, kbuf, ksiz)){
      eprint(adb, "tcadbout");
      err = true;
      break;
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tcadbrnum(adb));
  iprintf("size: %llu\n", (unsigned long long)tcadbsize(adb));
  if(!tcadbclose(adb)){
    eprint(adb, "tcadbclose");
    err = true;
  }
  tcadbdel(adb);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform rcat command */
static int procrcat(const char *name, int rnum){
  iprintf("<Random Concatenating Test>\n  name=%s  rnum=%d\n\n", name, rnum);
  int pnum = rnum / 5 + 1;
  bool err = false;
  double stime = tctime();
  TCADB *adb = tcadbnew();
  if(!tcadbopen(adb, name)){
    eprint(adb, "tcadbopen");
    err = true;
  }
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", myrand(pnum) + 1);
    if(!tcadbputcat(adb, kbuf, ksiz, kbuf, ksiz)){
      eprint(adb, "tcadbputcat");
      err = true;
      break;
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tcadbrnum(adb));
  iprintf("size: %llu\n", (unsigned long long)tcadbsize(adb));
  if(!tcadbclose(adb)){
    eprint(adb, "tcadbclose");
    err = true;
  }
  tcadbdel(adb);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform misc command */
static int procmisc(const char *name, int rnum){
  iprintf("<Miscellaneous Test>\n  name=%s  rnum=%d\n\n", name, rnum);
  bool err = false;
  double stime = tctime();
  TCADB *adb = tcadbnew();
  if(!tcadbopen(adb, name)){
    eprint(adb, "tcadbopen");
    err = true;
  }
  iprintf("writing:\n");
  for(int i = 1; i <= rnum; i++){
    char buf[RECBUFSIZ];
    int len = sprintf(buf, "%08d", i);
    if(!tcadbputkeep(adb, buf, len, buf, len)){
      eprint(adb, "tcadbputkeep");
      err = true;
      break;
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  iprintf("reading:\n");
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%08d", i);
    int vsiz;
    char *vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
    if(!vbuf){
      eprint(adb, "tcadbget");
      err = true;
      break;
    } else if(vsiz != ksiz || memcmp(vbuf, kbuf, vsiz)){
      eprint(adb, "(validation)");
      err = true;
      tcfree(vbuf);
      break;
    }
    tcfree(vbuf);
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  if(tcadbrnum(adb) != rnum){
    eprint(adb, "(validation)");
    err = true;
  }
  iprintf("random writing:\n");
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", myrand(rnum) + 1);
    char vbuf[RECBUFSIZ];
    int vsiz = myrand(RECBUFSIZ);
    memset(vbuf, '*', vsiz);
    if(!tcadbput(adb, kbuf, ksiz, vbuf, vsiz)){
      eprint(adb, "tcadbput");
      err = true;
      break;
    }
    int rsiz;
    char *rbuf = tcadbget(adb, kbuf, ksiz, &rsiz);
    if(!rbuf){
      eprint(adb, "tcadbget");
      err = true;
      break;
    }
    if(rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
      eprint(adb, "(validation)");
      err = true;
      tcfree(rbuf);
      break;
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
    tcfree(rbuf);
  }
  iprintf("word writing:\n");
  const char *words[] = {
    "a", "A", "bb", "BB", "ccc", "CCC", "dddd", "DDDD", "eeeee", "EEEEEE",
    "mikio", "hirabayashi", "tokyo", "cabinet", "hyper", "estraier", "19780211", "birth day",
    "one", "first", "two", "second", "three", "third", "four", "fourth", "five", "fifth",
    "_[1]_", "uno", "_[2]_", "dos", "_[3]_", "tres", "_[4]_", "cuatro", "_[5]_", "cinco",
    "[\xe5\xb9\xb3\xe6\x9e\x97\xe5\xb9\xb9\xe9\x9b\x84]", "[\xe9\xa6\xac\xe9\xb9\xbf]", NULL
  };
  for(int i = 0; words[i] != NULL; i += 2){
    const char *kbuf = words[i];
    int ksiz = strlen(kbuf);
    const char *vbuf = words[i+1];
    int vsiz = strlen(vbuf);
    if(!tcadbputkeep(adb, kbuf, ksiz, vbuf, vsiz)){
      eprint(adb, "tcadbputkeep");
      err = true;
      break;
    }
    if(rnum > 250) iputchar('.');
  }
  if(rnum > 250) iprintf(" (%08d)\n", sizeof(words) / sizeof(*words));
  iprintf("random erasing:\n");
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", myrand(rnum));
    tcadbout(adb, kbuf, ksiz);
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  iprintf("writing:\n");
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "[%d]", i);
    char vbuf[RECBUFSIZ];
    int vsiz = i % RECBUFSIZ;
    memset(vbuf, '*', vsiz);
    if(!tcadbputkeep(adb, kbuf, ksiz, vbuf, vsiz)){
      eprint(adb, "tcadbputkeep");
      err = true;
      break;
    }
    if(vsiz < 1){
      char tbuf[PATH_MAX];
      for(int j = 0; j < PATH_MAX; j++){
        tbuf[j] = myrand(0x100);
      }
      if(!tcadbput(adb, kbuf, ksiz, tbuf, PATH_MAX)){
        eprint(adb, "tcadbput");
        err = true;
        break;
      }
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  iprintf("erasing:\n");
  for(int i = 1; i <= rnum; i++){
    if(i % 2 == 1){
      char kbuf[RECBUFSIZ];
      int ksiz = sprintf(kbuf, "[%d]", i);
      if(!tcadbout(adb, kbuf, ksiz)){
        eprint(adb, "tcadbout");
        err = true;
        break;
      }
      tcadbout(adb, kbuf, ksiz);
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  iprintf("checking iterator:\n");
  if(!tcadbiterinit(adb)){
    eprint(adb, "tcadbiterinit");
    err = true;
  }
  char *kbuf;
  int ksiz;
  int inum = 0;
  for(int i = 1; (kbuf = tcadbiternext(adb, &ksiz)) != NULL; i++, inum++){
    int vsiz;
    char *vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
    if(!vbuf){
      eprint(adb, "tcadbget");
      err = true;
      tcfree(kbuf);
      break;
    }
    tcfree(vbuf);
    tcfree(kbuf);
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  if(rnum > 250) iprintf(" (%08d)\n", inum);
  if(inum != tcadbrnum(adb)){
    eprint(adb, "(validation)");
    err = true;
  }
  iprintf("checking versatile functions:\n");
  TCLIST *args = tclistnew();
  for(int i = 1; i <= rnum; i++){
    if(myrand(10) == 0){
      const char *name;
      switch(myrand(3)){
      default: name = "putlist"; break;
      case 1: name = "outlist"; break;
      case 2: name = "getlist"; break;
      }
      TCLIST *rv = tcadbmisc(adb, name, args);
      if(rv){
        tclistdel(rv);
      } else {
        eprint(adb, "tcadbmisc");
        err = true;
        break;
      }
    } else {
      char kbuf[RECBUFSIZ];
      int ksiz = sprintf(kbuf, "(%d)", i);
      tclistpush(args, kbuf, ksiz);
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  tclistdel(args);
  if(!tcadbsync(adb)){
    eprint(adb, "tcadbsync");
    err = true;
  }
  if(!tcadbvanish(adb)){
    eprint(adb, "tcadbvanish");
    err = true;
  }
  if(!tcadbput2(adb, "mikio", "hirabayashi")){
    eprint(adb, "tcadbput2");
    err = true;
  }
  for(int i = 0; i < 10; i++){
    char buf[RECBUFSIZ];
    int size = sprintf(buf, "%d", myrand(rnum));
    if(!tcadbput(adb, buf, size, buf, size)){
      eprint(adb, "tcadbput");
      err = true;
    }
  }
  for(int i = myrand(3) + 1; i < PATH_MAX; i = i * 2 + myrand(3)){
    char vbuf[i];
    memset(vbuf, '@', i - 1);
    vbuf[i-1] = '\0';
    if(!tcadbput2(adb, "mikio", vbuf)){
      eprint(adb, "tcadbput2");
      err = true;
    }
  }
  if(!tcadbforeach(adb, iterfunc, NULL)){
    eprint(adb, "tcadbforeach");
    err = true;
  }
  iprintf("record number: %llu\n", (unsigned long long)tcadbrnum(adb));
  iprintf("size: %llu\n", (unsigned long long)tcadbsize(adb));
  if(!tcadbclose(adb)){
    eprint(adb, "tcadbclose");
    err = true;
  }
  tcadbdel(adb);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform wicked command */
static int procwicked(const char *name, int rnum){
  iprintf("<Wicked Writing Test>\n  name=%s  rnum=%d\n\n", name, rnum);
  bool err = false;
  double stime = tctime();
  TCADB *adb = tcadbnew();
  if(!tcadbopen(adb, name)){
    eprint(adb, "tcadbopen");
    err = true;
  }
  TCMAP *map = tcmapnew2(rnum / 5);
  for(int i = 1; i <= rnum && !err; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", myrand(rnum) + 1);
    char vbuf[RECBUFSIZ];
    int vsiz = myrand(RECBUFSIZ);
    memset(vbuf, '*', vsiz);
    vbuf[vsiz] = '\0';
    char *rbuf;
    switch(myrand(16)){
    case 0:
      iputchar('0');
      if(!tcadbput(adb, kbuf, ksiz, vbuf, vsiz)){
        eprint(adb, "tcadbput");
        err = true;
      }
      tcmapput(map, kbuf, ksiz, vbuf, vsiz);
      break;
    case 1:
      iputchar('1');
      if(!tcadbput2(adb, kbuf, vbuf)){
        eprint(adb, "tcadbput2");
        err = true;
      }
      tcmapput2(map, kbuf, vbuf);
      break;
    case 2:
      iputchar('2');
      tcadbputkeep(adb, kbuf, ksiz, vbuf, vsiz);
      tcmapputkeep(map, kbuf, ksiz, vbuf, vsiz);
      break;
    case 3:
      iputchar('3');
      tcadbputkeep2(adb, kbuf, vbuf);
      tcmapputkeep2(map, kbuf, vbuf);
      break;
    case 4:
      iputchar('4');
      if(!tcadbputcat(adb, kbuf, ksiz, vbuf, vsiz)){
        eprint(adb, "tcadbputcat");
        err = true;
      }
      tcmapputcat(map, kbuf, ksiz, vbuf, vsiz);
      break;
    case 5:
      iputchar('5');
      if(!tcadbputcat2(adb, kbuf, vbuf)){
        eprint(adb, "tcadbputcat2");
        err = true;
      }
      tcmapputcat2(map, kbuf, vbuf);
      break;
    case 6:
      iputchar('6');
      if(myrand(10) == 0){
        tcadbout(adb, kbuf, ksiz);
        tcmapout(map, kbuf, ksiz);
      }
      break;
    case 7:
      iputchar('7');
      if(myrand(10) == 0){
        tcadbout2(adb, kbuf);
        tcmapout2(map, kbuf);
      }
      break;
    case 8:
      iputchar('8');
      if((rbuf = tcadbget(adb, kbuf, ksiz, &vsiz)) != NULL) tcfree(rbuf);
      break;
    case 9:
      iputchar('9');
      if((rbuf = tcadbget2(adb, kbuf)) != NULL) tcfree(rbuf);
      break;
    case 10:
      iputchar('A');
      tcadbvsiz(adb, kbuf, ksiz);
      break;
    case 11:
      iputchar('B');
      tcadbvsiz2(adb, kbuf);
      break;
    case 12:
      iputchar('E');
      if(myrand(rnum / 50) == 0){
        if(!tcadbiterinit(adb)){
          eprint(adb, "tcadbiterinit");
          err = true;
        }
      }
      for(int j = myrand(rnum) / 1000 + 1; j >= 0; j--){
        int iksiz;
        char *ikbuf = tcadbiternext(adb, &iksiz);
        if(ikbuf) tcfree(ikbuf);
      }
      break;
    default:
      iputchar('@');
      if(myrand(10000) == 0) srand((unsigned int)(tctime() * 1000) % UINT_MAX);
      break;
    }
    if(i % 50 == 0) iprintf(" (%08d)\n", i);
  }
  if(rnum % 50 > 0) iprintf(" (%08d)\n", rnum);
  tcadbsync(adb);
  if(tcadbrnum(adb) != tcmaprnum(map)){
    eprint(adb, "(validation)");
    err = true;
  }
  for(int i = 1; i <= rnum && !err; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", i - 1);
    int vsiz;
    const char *vbuf = tcmapget(map, kbuf, ksiz, &vsiz);
    int rsiz;
    char *rbuf = tcadbget(adb, kbuf, ksiz, &rsiz);
    if(vbuf){
      iputchar('.');
      if(!rbuf){
        eprint(adb, "tcadbget");
        err = true;
      } else if(rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
        eprint(adb, "(validation)");
        err = true;
      }
    } else {
      iputchar('*');
      if(rbuf){
        eprint(adb, "(validation)");
        err = true;
      }
    }
    tcfree(rbuf);
    if(i % 50 == 0) iprintf(" (%08d)\n", i);
  }
  if(rnum % 50 > 0) iprintf(" (%08d)\n", rnum);
  tcmapiterinit(map);
  int ksiz;
  const char *kbuf;
  for(int i = 1; (kbuf = tcmapiternext(map, &ksiz)) != NULL; i++){
    iputchar('+');
    int vsiz;
    const char *vbuf = tcmapiterval(kbuf, &vsiz);
    int rsiz;
    char *rbuf = tcadbget(adb, kbuf, ksiz, &rsiz);
    if(!rbuf){
      eprint(adb, "tcadbget");
      err = true;
    } else if(rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
      eprint(adb, "(validation)");
      err = true;
    }
    tcfree(rbuf);
    if(!tcadbout(adb, kbuf, ksiz)){
      eprint(adb, "tcadbout");
      err = true;
    }
    if(i % 50 == 0) iprintf(" (%08d)\n", i);
  }
  int mrnum = tcmaprnum(map);
  if(mrnum % 50 > 0) iprintf(" (%08d)\n", mrnum);
  if(tcadbrnum(adb) != 0){
    eprint(adb, "(validation)");
    err = true;
  }
  iprintf("record number: %llu\n", (unsigned long long)tcadbrnum(adb));
  iprintf("size: %llu\n", (unsigned long long)tcadbsize(adb));
  tcmapdel(map);
  if(!tcadbclose(adb)){
    eprint(adb, "tcadbclose");
    err = true;
  }
  tcadbdel(adb);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform compare command */
static int proccompare(const char *name, int tnum, int rnum){
  iprintf("<Comparison Test>\n  name=%s  tnum=%d  rnum=%d\n\n", name, tnum, rnum);
  bool err = false;
  double stime = tctime();
  char path[PATH_MAX];
  TCMDB *mdb = tcmdbnew2(rnum / 2);
  TCNDB *ndb = tcndbnew();
  TCHDB *hdb = tchdbnew();
  int hopts = 0;
  if(myrand(2) == 1) hopts |= HDBTLARGE;
  if(myrand(2) == 1) hopts |= HDBTBZIP;
  if(!tchdbtune(hdb, rnum / 2, -1, -1, hopts)){
    eprint(NULL, "tchdbtune");
    err = true;
  }
  if(!tchdbsetcache(hdb, rnum / 10)){
    eprint(NULL, "tchdbsetcache");
    err = true;
  }
  if(!tchdbsetxmsiz(hdb, 4096)){
    eprint(NULL, "tchdbsetxmsiz");
    err = true;
  }
  sprintf(path, "%s.tch", name);
  int homode = HDBOWRITER | HDBOCREAT | HDBOTRUNC;
  if(myrand(5) == 1) homode |= HDBOTSYNC;
  if(!tchdbopen(hdb, path, homode)){
    eprint(NULL, "tchdbopen");
    err = true;
  }
  TCBDB *bdb = tcbdbnew();
  int bopts = 0;
  if(myrand(2) == 1) bopts |= BDBTLARGE;
  if(myrand(2) == 1) bopts |= BDBTBZIP;
  if(!tcbdbtune(bdb, 5, 5, rnum / 10, -1, -1, bopts)){
    eprint(NULL, "tcbdbtune");
    err = true;
  }
  sprintf(path, "%s.tcb", name);
  int bomode = BDBOWRITER | BDBOCREAT | BDBOTRUNC;
  if(myrand(5) == 1) bomode |= BDBOTSYNC;
  if(!tcbdbopen(bdb, path, bomode)){
    eprint(NULL, "tcbdbopen");
    err = true;
  }
  for(int t = 1; !err && t <= tnum; t++){
    bool commit = myrand(2) == 0;
    iprintf("transaction %d (%s):\n", t, commit ? "commit" : "abort");
    if(!tchdbtranbegin(hdb)){
      eprint(NULL, "tchdbtranbegin");
      err = true;
    }
    if(!tcbdbtranbegin(bdb)){
      eprint(NULL, "tcbdbtranbegin");
      err = true;
    }
    int act = myrand(7);
    for(int i = 1; !err && i <= rnum; i++){
      if(myrand(10) == 0) act = myrand(7);
      char kbuf[RECBUFSIZ];
      int ksiz = sprintf(kbuf, "%d", myrand(i) + 1);
      char vbuf[RECBUFSIZ+256];
      int vsiz = sprintf(vbuf, "%64d:%d:%d", t, i, myrand(i));
      switch(act){
      case 0:
        if(!tchdbput(hdb, kbuf, ksiz, vbuf, vsiz)){
          eprint(NULL, "tchdbput");
          err = true;
        }
        if(!tcbdbput(bdb, kbuf, ksiz, vbuf, vsiz)){
          eprint(NULL, "tcbdbput");
          err = true;
        }
        if(commit){
          tcmdbput(mdb, kbuf, ksiz, vbuf, vsiz);
          tcndbput(ndb, kbuf, ksiz, vbuf, vsiz);
        }
        break;
      case 1:
        if(!tchdbputkeep(hdb, kbuf, ksiz, vbuf, vsiz) && tchdbecode(hdb) != TCEKEEP){
          eprint(NULL, "tchdbputkeep");
          err = true;
        }
        if(!tcbdbputkeep(bdb, kbuf, ksiz, vbuf, vsiz) && tcbdbecode(bdb) != TCEKEEP){
          eprint(NULL, "tcbdbputkeep");
          err = true;
        }
        if(commit){
          tcmdbputkeep(mdb, kbuf, ksiz, vbuf, vsiz);
          tcndbputkeep(ndb, kbuf, ksiz, vbuf, vsiz);
        }
        break;
      case 2:
        if(!tchdbputcat(hdb, kbuf, ksiz, vbuf, vsiz)){
          eprint(NULL, "tchdbputcat");
          err = true;
        }
        if(!tcbdbputcat(bdb, kbuf, ksiz, vbuf, vsiz)){
          eprint(NULL, "tcbdbputcat");
          err = true;
        }
        if(commit){
          tcmdbputcat(mdb, kbuf, ksiz, vbuf, vsiz);
          tcndbputcat(ndb, kbuf, ksiz, vbuf, vsiz);
        }
        break;
      case 3:
        if(tchdbaddint(hdb, kbuf, ksiz, 1) == INT_MIN && tchdbecode(hdb) != TCEKEEP){
          eprint(NULL, "tchdbaddint");
          err = true;
        }
        if(tcbdbaddint(bdb, kbuf, ksiz, 1) == INT_MIN && tcbdbecode(bdb) != TCEKEEP){
          eprint(NULL, "tchdbaddint");
          err = true;
        }
        if(commit){
          tcmdbaddint(mdb, kbuf, ksiz, 1);
          tcndbaddint(ndb, kbuf, ksiz, 1);
        }
        break;
      case 4:
        if(isnan(tchdbadddouble(hdb, kbuf, ksiz, 1.0)) && tchdbecode(hdb) != TCEKEEP){
          eprint(NULL, "tchdbadddouble");
          err = true;
        }
        if(isnan(tcbdbadddouble(bdb, kbuf, ksiz, 1.0)) && tcbdbecode(bdb) != TCEKEEP){
          eprint(NULL, "tchdbadddouble");
          err = true;
        }
        if(commit){
          tcmdbadddouble(mdb, kbuf, ksiz, 1.0);
          tcndbadddouble(ndb, kbuf, ksiz, 1.0);
        }
        break;
      default:
        if(!tchdbout(hdb, kbuf, ksiz) && tchdbecode(hdb) != TCENOREC){
          eprint(NULL, "tchdbout");
          err = true;
        }
        if(!tcbdbout(bdb, kbuf, ksiz) && tcbdbecode(bdb) != TCENOREC){
          eprint(NULL, "tcbdbout");
          err = true;
        }
        if(commit){
          tcmdbout(mdb, kbuf, ksiz);
          tcndbout(ndb, kbuf, ksiz);
        }
        break;
      }
      if(rnum > 250 && i % (rnum / 250) == 0){
        iputchar('.');
        if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
      }
    }
    if(commit){
      if(!tchdbtrancommit(hdb)){
        eprint(NULL, "tchdbcommit");
        err = true;
      }
      if(!tcbdbtrancommit(bdb)){
        eprint(NULL, "tcbdbcommit");
        err = true;
      }
    } else {
      if(myrand(5) == 0){
        if(!tchdbclose(hdb)){
          eprint(NULL, "tchdbclose");
          err = true;
        }
        sprintf(path, "%s.tch", name);
        if(!tchdbopen(hdb, path, HDBOWRITER)){
          eprint(NULL, "tchdbopen");
          err = true;
        }
        if(!tcbdbclose(bdb)){
          eprint(NULL, "tcbdbclose");
          err = true;
        }
        sprintf(path, "%s.tcb", name);
        if(!tcbdbopen(bdb, path, BDBOWRITER)){
          eprint(NULL, "tcbdbopen");
          err = true;
        }
      } else {
        if(!tchdbtranabort(hdb)){
          eprint(NULL, "tchdbtranabort");
          err = true;
        }
        if(!tcbdbtranabort(bdb)){
          eprint(NULL, "tcbdbtranabort");
          err = true;
        }
      }
    }
  }
  iprintf("checking consistency of all:\n");
  if(tchdbrnum(hdb) != tcbdbrnum(bdb)){
    eprint(NULL, "(validation)");
    err = true;
  }
  int inum = 0;
  tcmdbiterinit(mdb);
  char *kbuf;
  int ksiz;
  for(int i = 1; (kbuf = tcmdbiternext(mdb, &ksiz)) != NULL; i++, inum++){
    int vsiz;
    char *vbuf = tcmdbget(mdb, kbuf, ksiz, &vsiz);
    int rsiz;
    char *rbuf = tcndbget(ndb, kbuf, ksiz, &rsiz);
    if(!rbuf || rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
      eprint(NULL, "tcndbget");
      err = true;
    }
    tcfree(rbuf);
    rbuf = tchdbget(hdb, kbuf, ksiz, &rsiz);
    if(!rbuf || rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
      eprint(NULL, "tchdbget");
      err = true;
    }
    tcfree(rbuf);
    rbuf = tcbdbget(bdb, kbuf, ksiz, &rsiz);
    if(!rbuf || rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
      eprint(NULL, "tcbdbget");
      err = true;
    }
    tcfree(rbuf);
    tcfree(vbuf);
    tcfree(kbuf);
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  if(rnum > 250) iprintf(" (%08d)\n", inum);
  iprintf("checking consistency of hash:\n");
  inum = 0;
  if(!tchdbiterinit(hdb)){
    eprint(NULL, "tchdbiterinit");
    err = true;
  }
  for(int i = 1; (kbuf = tchdbiternext(hdb, &ksiz)) != NULL; i++, inum++){
    int vsiz;
    char *vbuf = tchdbget(hdb, kbuf, ksiz, &vsiz);
    int rsiz;
    char *rbuf = tcmdbget(mdb, kbuf, ksiz, &rsiz);
    if(!rbuf || rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
      eprint(NULL, "(validation)");
      err = true;
    }
    tcfree(rbuf);
    tcfree(vbuf);
    tcfree(kbuf);
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  if(rnum > 250) iprintf(" (%08d)\n", inum);
  iprintf("checking consistency of tree:\n");
  BDBCUR *cur = tcbdbcurnew(bdb);
  if(!tcbdbcurfirst(cur) && tcbdbecode(bdb) != TCENOREC){
    eprint(NULL, "tcbdbcurfirst");
    err = true;
  }
  for(int i = 1; (kbuf = tcbdbcurkey(cur, &ksiz)) != NULL; i++, inum++){
    int vsiz;
    char *vbuf = tcbdbget(bdb, kbuf, ksiz, &vsiz);
    int rsiz;
    char *rbuf = tcndbget(ndb, kbuf, ksiz, &rsiz);
    if(!rbuf || rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
      eprint(NULL, "(validation)");
      err = true;
    }
    tcfree(rbuf);
    tcfree(vbuf);
    tcfree(kbuf);
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
    tcbdbcurnext(cur);
  }
  if(rnum > 250) iprintf(" (%08d)\n", inum);
  tcbdbcurdel(cur);
  if(!tchdbclose(hdb)){
    eprint(NULL, "tcbdbclose");
    err = true;
  }
  if(!tcbdbclose(bdb)){
    eprint(NULL, "tcbdbclose");
    err = true;
  }
  tcmdbdel(mdb);
  tcndbdel(ndb);
  tchdbdel(hdb);
  tcbdbdel(bdb);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}



// END OF FILE
