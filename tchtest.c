/*************************************************************************************************
 * The test cases of the hash database API
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
#include <tchdb.h>
#include "myconf.h"

#define RECBUFSIZ      32                // buffer for records


/* global variables */
const char *g_progname;                  // program name
int g_dbgfd;                             // debugging output


/* function prototypes */
int main(int argc, char **argv);
static void usage(void);
static void iprintf(const char *format, ...);
static void iputchar(int c);
static void eprint(TCHDB *hdb, const char *func);
static void mprint(TCHDB *hdb);
static bool iterfunc(const void *kbuf, int ksiz, const void *vbuf, int vsiz, void *op);
static int myrand(int range);
static int runwrite(int argc, char **argv);
static int runread(int argc, char **argv);
static int runremove(int argc, char **argv);
static int runrcat(int argc, char **argv);
static int runmisc(int argc, char **argv);
static int runwicked(int argc, char **argv);
static int procwrite(const char *path, int rnum, int bnum, int apow, int fpow,
                     bool mt, int opts, int rcnum, int xmsiz, int omode, bool as, bool rnd);
static int procread(const char *path, bool mt, int rcnum, int xmsiz, int omode,
                    bool wb, bool rnd);
static int procremove(const char *path, bool mt, int rcnum, int xmsiz, int omode, bool rnd);
static int procrcat(const char *path, int rnum, int bnum, int apow, int fpow,
                    bool mt, int opts, int rcnum, int xmsiz, int omode, int pnum,
                    bool dai, bool dad, bool rl);
static int procmisc(const char *path, int rnum, bool mt, int opts, int omode);
static int procwicked(const char *path, int rnum, bool mt, int opts, int omode);


/* main routine */
int main(int argc, char **argv){
  g_progname = argv[0];
  g_dbgfd = -1;
  const char *ebuf = getenv("TCDBGFD");
  if(ebuf) g_dbgfd = tcatoi(ebuf);
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
  } else {
    usage();
  }
  return rv;
}


/* print the usage and exit */
static void usage(void){
  fprintf(stderr, "%s: test cases of the hash database API of Tokyo Cabinet\n", g_progname);
  fprintf(stderr, "\n");
  fprintf(stderr, "usage:\n");
  fprintf(stderr, "  %s write [-mt] [-tl] [-td|-tb|-tt|-tx] [-rc num] [-xm num] [-nl|-nb]"
          " [-as] [-rnd] path rnum [bnum [apow [fpow]]]\n", g_progname);
  fprintf(stderr, "  %s read [-mt] [-rc num] [-xm num] [-nl|-nb] [-wb] [-rnd] path\n",
          g_progname);
  fprintf(stderr, "  %s remove [-mt] [-rc num] [-xm num] [-nl|-nb] [-rnd] path\n", g_progname);
  fprintf(stderr, "  %s rcat [-mt] [-rc num] [-xm num] [-tl] [-td|-tb|-tt|-tx] [-nl|-nb]"
          " [-pn num] [-dai|-dad|-rl] path rnum [bnum [apow [fpow]]]\n", g_progname);
  fprintf(stderr, "  %s misc [-mt] [-tl] [-td|-tb|-tt|-tx] [-nl|-nb] path rnum\n", g_progname);
  fprintf(stderr, "  %s wicked [-mt] [-tl] [-td|-tb|-tt|-tx] [-nl|-nb] path rnum\n", g_progname);
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


/* print error message of hash database */
static void eprint(TCHDB *hdb, const char *func){
  const char *path = tchdbpath(hdb);
  int ecode = tchdbecode(hdb);
  fprintf(stderr, "%s: %s: %s: error: %d: %s\n",
          g_progname, path ? path : "-", func, ecode, tchdberrmsg(ecode));
}


/* print members of hash database */
static void mprint(TCHDB *hdb){
  if(hdb->cnt_writerec < 0) return;
  iprintf("bucket number: %lld\n", (long long)tchdbbnum(hdb));
  iprintf("used bucket number: %lld\n", (long long)tchdbbnumused(hdb));
  iprintf("cnt_writerec: %lld\n", (long long)hdb->cnt_writerec);
  iprintf("cnt_reuserec: %lld\n", (long long)hdb->cnt_reuserec);
  iprintf("cnt_moverec: %lld\n", (long long)hdb->cnt_moverec);
  iprintf("cnt_readrec: %lld\n", (long long)hdb->cnt_readrec);
  iprintf("cnt_searchfbp: %lld\n", (long long)hdb->cnt_searchfbp);
  iprintf("cnt_insertfbp: %lld\n", (long long)hdb->cnt_insertfbp);
  iprintf("cnt_splicefbp: %lld\n", (long long)hdb->cnt_splicefbp);
  iprintf("cnt_dividefbp: %lld\n", (long long)hdb->cnt_dividefbp);
  iprintf("cnt_mergefbp: %lld\n", (long long)hdb->cnt_mergefbp);
  iprintf("cnt_reducefbp: %lld\n", (long long)hdb->cnt_reducefbp);
  iprintf("cnt_appenddrp: %lld\n", (long long)hdb->cnt_appenddrp);
  iprintf("cnt_deferdrp: %lld\n", (long long)hdb->cnt_deferdrp);
  iprintf("cnt_flushdrp: %lld\n", (long long)hdb->cnt_flushdrp);
  iprintf("cnt_adjrecc: %lld\n", (long long)hdb->cnt_adjrecc);
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
  char *path = NULL;
  char *rstr = NULL;
  char *bstr = NULL;
  char *astr = NULL;
  char *fstr = NULL;
  bool mt = false;
  int opts = 0;
  int rcnum = 0;
  int xmsiz = -1;
  int omode = 0;
  bool as = false;
  bool rnd = false;
  for(int i = 2; i < argc; i++){
    if(!path && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-mt")){
        mt = true;
      } else if(!strcmp(argv[i], "-tl")){
        opts |= HDBTLARGE;
      } else if(!strcmp(argv[i], "-td")){
        opts |= HDBTDEFLATE;
      } else if(!strcmp(argv[i], "-tb")){
        opts |= HDBTBZIP;
      } else if(!strcmp(argv[i], "-tt")){
        opts |= HDBTTCBS;
      } else if(!strcmp(argv[i], "-tx")){
        opts |= HDBTEXCODEC;
      } else if(!strcmp(argv[i], "-rc")){
        if(++i >= argc) usage();
        rcnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-xm")){
        if(++i >= argc) usage();
        xmsiz = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-nl")){
        omode |= HDBONOLCK;
      } else if(!strcmp(argv[i], "-nb")){
        omode |= HDBOLCKNB;
      } else if(!strcmp(argv[i], "-as")){
        as = true;
      } else if(!strcmp(argv[i], "-rnd")){
        rnd = true;
      } else {
        usage();
      }
    } else if(!path){
      path = argv[i];
    } else if(!rstr){
      rstr = argv[i];
    } else if(!bstr){
      bstr = argv[i];
    } else if(!astr){
      astr = argv[i];
    } else if(!fstr){
      fstr = argv[i];
    } else {
      usage();
    }
  }
  if(!path || !rstr) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int bnum = bstr ? tcatoi(bstr) : -1;
  int apow = astr ? tcatoi(astr) : -1;
  int fpow = fstr ? tcatoi(fstr) : -1;
  int rv = procwrite(path, rnum, bnum, apow, fpow, mt, opts, rcnum, xmsiz, omode, as, rnd);
  return rv;
}


/* parse arguments of read command */
static int runread(int argc, char **argv){
  char *path = NULL;
  bool mt = false;
  int rcnum = 0;
  int xmsiz = -1;
  int omode = 0;
  bool wb = false;
  bool rnd = false;
  for(int i = 2; i < argc; i++){
    if(!path && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-mt")){
        mt = true;
      } else if(!strcmp(argv[i], "-rc")){
        if(++i >= argc) usage();
        rcnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-xm")){
        if(++i >= argc) usage();
        xmsiz = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-nl")){
        omode |= HDBONOLCK;
      } else if(!strcmp(argv[i], "-nb")){
        omode |= HDBOLCKNB;
      } else if(!strcmp(argv[i], "-wb")){
        wb = true;
      } else if(!strcmp(argv[i], "-rnd")){
        rnd = true;
      } else {
        usage();
      }
    } else if(!path){
      path = argv[i];
    } else {
      usage();
    }
  }
  if(!path) usage();
  int rv = procread(path, mt, rcnum, xmsiz, omode, wb, rnd);
  return rv;
}


/* parse arguments of remove command */
static int runremove(int argc, char **argv){
  char *path = NULL;
  bool mt = false;
  int rcnum = 0;
  int xmsiz = -1;
  int omode = 0;
  bool rnd = false;
  for(int i = 2; i < argc; i++){
    if(!path && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-mt")){
        mt = true;
      } else if(!strcmp(argv[i], "-rc")){
        if(++i >= argc) usage();
        rcnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-xm")){
        if(++i >= argc) usage();
        xmsiz = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-nl")){
        omode |= HDBONOLCK;
      } else if(!strcmp(argv[i], "-nb")){
        omode |= HDBOLCKNB;
      } else if(!strcmp(argv[i], "-rnd")){
        rnd = true;
      } else {
        usage();
      }
    } else if(!path){
      path = argv[i];
    } else {
      usage();
    }
  }
  if(!path) usage();
  int rv = procremove(path, mt, rcnum, xmsiz, omode, rnd);
  return rv;
}


/* parse arguments of rcat command */
static int runrcat(int argc, char **argv){
  char *path = NULL;
  char *rstr = NULL;
  char *bstr = NULL;
  char *astr = NULL;
  char *fstr = NULL;
  bool mt = false;
  int opts = 0;
  int rcnum = 0;
  int xmsiz = -1;
  int omode = 0;
  int pnum = 0;
  bool dai = false;
  bool dad = false;
  bool rl = false;
  for(int i = 2; i < argc; i++){
    if(!path && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-mt")){
        mt = true;
      } else if(!strcmp(argv[i], "-tl")){
        opts |= HDBTLARGE;
      } else if(!strcmp(argv[i], "-td")){
        opts |= HDBTDEFLATE;
      } else if(!strcmp(argv[i], "-tb")){
        opts |= HDBTBZIP;
      } else if(!strcmp(argv[i], "-tt")){
        opts |= HDBTTCBS;
      } else if(!strcmp(argv[i], "-tx")){
        opts |= HDBTEXCODEC;
      } else if(!strcmp(argv[i], "-rc")){
        if(++i >= argc) usage();
        rcnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-xm")){
        if(++i >= argc) usage();
        xmsiz = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-nl")){
        omode |= HDBONOLCK;
      } else if(!strcmp(argv[i], "-nb")){
        omode |= HDBOLCKNB;
      } else if(!strcmp(argv[i], "-pn")){
        if(++i >= argc) usage();
        pnum = tcatoi(argv[i]);
      } else if(!strcmp(argv[i], "-dai")){
        dai = true;
      } else if(!strcmp(argv[i], "-dad")){
        dad = true;
      } else if(!strcmp(argv[i], "-rl")){
        rl = true;
      } else {
        usage();
      }
    } else if(!path){
      path = argv[i];
    } else if(!rstr){
      rstr = argv[i];
    } else if(!bstr){
      bstr = argv[i];
    } else if(!astr){
      astr = argv[i];
    } else if(!fstr){
      fstr = argv[i];
    } else {
      usage();
    }
  }
  if(!path || !rstr) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int bnum = bstr ? tcatoi(bstr) : -1;
  int apow = astr ? tcatoi(astr) : -1;
  int fpow = fstr ? tcatoi(fstr) : -1;
  int rv = procrcat(path, rnum, bnum, apow, fpow, mt, opts, rcnum, xmsiz, omode, pnum,
                    dai, dad, rl);
  return rv;
}


/* parse arguments of misc command */
static int runmisc(int argc, char **argv){
  char *path = NULL;
  char *rstr = NULL;
  bool mt = false;
  int opts = 0;
  int omode = 0;
  for(int i = 2; i < argc; i++){
    if(!path && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-mt")){
        mt = true;
      } else if(!strcmp(argv[i], "-tl")){
        opts |= HDBTLARGE;
      } else if(!strcmp(argv[i], "-td")){
        opts |= HDBTDEFLATE;
      } else if(!strcmp(argv[i], "-tb")){
        opts |= HDBTBZIP;
      } else if(!strcmp(argv[i], "-tt")){
        opts |= HDBTTCBS;
      } else if(!strcmp(argv[i], "-tx")){
        opts |= HDBTEXCODEC;
      } else if(!strcmp(argv[i], "-nl")){
        omode |= HDBONOLCK;
      } else if(!strcmp(argv[i], "-nb")){
        omode |= HDBOLCKNB;
      } else {
        usage();
      }
    } else if(!path){
      path = argv[i];
    } else if(!rstr){
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if(!path || !rstr) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int rv = procmisc(path, rnum, mt, opts, omode);
  return rv;
}


/* parse arguments of wicked command */
static int runwicked(int argc, char **argv){
  char *path = NULL;
  char *rstr = NULL;
  bool mt = false;
  int opts = 0;
  int omode = 0;
  for(int i = 2; i < argc; i++){
    if(!path && argv[i][0] == '-'){
      if(!strcmp(argv[i], "-mt")){
        mt = true;
      } else if(!strcmp(argv[i], "-tl")){
        opts |= HDBTLARGE;
      } else if(!strcmp(argv[i], "-td")){
        opts |= HDBTDEFLATE;
      } else if(!strcmp(argv[i], "-tb")){
        opts |= HDBTBZIP;
      } else if(!strcmp(argv[i], "-tt")){
        opts |= HDBTTCBS;
      } else if(!strcmp(argv[i], "-tx")){
        opts |= HDBTEXCODEC;
      } else if(!strcmp(argv[i], "-nl")){
        omode |= HDBONOLCK;
      } else if(!strcmp(argv[i], "-nb")){
        omode |= HDBOLCKNB;
      } else {
        usage();
      }
    } else if(!path){
      path = argv[i];
    } else if(!rstr){
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if(!path || !rstr) usage();
  int rnum = tcatoi(rstr);
  if(rnum < 1) usage();
  int rv = procwicked(path, rnum, mt, opts, omode);
  return rv;
}


/* perform write command */
static int procwrite(const char *path, int rnum, int bnum, int apow, int fpow,
                     bool mt, int opts, int rcnum, int xmsiz, int omode, bool as, bool rnd){
  iprintf("<Writing Test>\n  path=%s  rnum=%d  bnum=%d  apow=%d  fpow=%d  mt=%d"
          "  opts=%d  rcnum=%d  xmsiz=%d  omode=%d  as=%d  rnd=%d\n\n",
          path, rnum, bnum, apow, fpow, mt, opts, rcnum, xmsiz, omode, as, rnd);
  bool err = false;
  double stime = tctime();
  TCHDB *hdb = tchdbnew();
  if(g_dbgfd >= 0) tchdbsetdbgfd(hdb, g_dbgfd);
  if(mt && !tchdbsetmutex(hdb)){
    eprint(hdb, "tchdbsetmutex");
    err = true;
  }
  if(!tchdbsetcodecfunc(hdb, _tc_recencode, NULL, _tc_recdecode, NULL)){
    eprint(hdb, "tchdbsetcodecfunc");
    err = true;
  }
  if(!tchdbtune(hdb, bnum, apow, fpow, opts)){
    eprint(hdb, "tchdbtune");
    err = true;
  }
  if(!tchdbsetcache(hdb, rcnum)){
    eprint(hdb, "tchdbsetcache");
    err = true;
  }
  if(xmsiz >= 0 && !tchdbsetxmsiz(hdb, xmsiz)){
    eprint(hdb, "tchdbsetxmsiz");
    err = true;
  }
  if(!rnd) omode |= HDBOTRUNC;
  if(!tchdbopen(hdb, path, HDBOWRITER | HDBOCREAT | omode)){
    eprint(hdb, "tchdbopen");
    err = true;
  }
  for(int i = 1; i <= rnum; i++){
    char buf[RECBUFSIZ];
    int len = sprintf(buf, "%08d", rnd ? myrand(rnum) + 1 : i);
    if(as){
      if(!tchdbputasync(hdb, buf, len, buf, len)){
        eprint(hdb, "tchdbput");
        err = true;
        break;
      }
    } else {
      if(!tchdbput(hdb, buf, len, buf, len)){
        eprint(hdb, "tchdbput");
        err = true;
        break;
      }
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tchdbrnum(hdb));
  iprintf("size: %llu\n", (unsigned long long)tchdbfsiz(hdb));
  mprint(hdb);
  if(!tchdbclose(hdb)){
    eprint(hdb, "tchdbclose");
    err = true;
  }
  tchdbdel(hdb);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform read command */
static int procread(const char *path, bool mt, int rcnum, int xmsiz, int omode,
                    bool wb, bool rnd){
  iprintf("<Reading Test>\n  path=%s  mt=%d  rcnum=%d  xmsiz=%d  omode=%d  wb=%d  rnd=%d\n\n",
          path, mt, rcnum, xmsiz, omode, wb, rnd);
  bool err = false;
  double stime = tctime();
  TCHDB *hdb = tchdbnew();
  if(g_dbgfd >= 0) tchdbsetdbgfd(hdb, g_dbgfd);
  if(mt && !tchdbsetmutex(hdb)){
    eprint(hdb, "tchdbsetmutex");
    err = true;
  }
  if(!tchdbsetcodecfunc(hdb, _tc_recencode, NULL, _tc_recdecode, NULL)){
    eprint(hdb, "tchdbsetcodecfunc");
    err = true;
  }
  if(!tchdbsetcache(hdb, rcnum)){
    eprint(hdb, "tchdbsetcache");
    err = true;
  }
  if(xmsiz >= 0 && !tchdbsetxmsiz(hdb, xmsiz)){
    eprint(hdb, "tchdbsetxmsiz");
    err = true;
  }
  if(!tchdbopen(hdb, path, HDBOREADER | omode)){
    eprint(hdb, "tchdbopen");
    err = true;
  }
  int rnum = tchdbrnum(hdb);
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%08d", rnd ? myrand(rnum) + 1 : i);
    int vsiz;
    if(wb){
      char vbuf[RECBUFSIZ];
      int vsiz = tchdbget3(hdb, kbuf, ksiz, vbuf, RECBUFSIZ);
      if(vsiz < 0 && !(rnd && tchdbecode(hdb) == TCENOREC)){
        eprint(hdb, "tchdbget3");
        err = true;
        break;
      }
    } else {
      char *vbuf = tchdbget(hdb, kbuf, ksiz, &vsiz);
      if(!vbuf && !(rnd && tchdbecode(hdb) == TCENOREC)){
        eprint(hdb, "tchdbget");
        err = true;
        break;
      }
      tcfree(vbuf);
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tchdbrnum(hdb));
  iprintf("size: %llu\n", (unsigned long long)tchdbfsiz(hdb));
  mprint(hdb);
  if(!tchdbclose(hdb)){
    eprint(hdb, "tchdbclose");
    err = true;
  }
  tchdbdel(hdb);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform remove command */
static int procremove(const char *path, bool mt, int rcnum, int xmsiz, int omode, bool rnd){
  iprintf("<Removing Test>\n  path=%s  mt=%d  rcnum=%d  xmsiz=%d  omode=%d  rnd=%d\n\n",
          path, mt, rcnum, xmsiz, omode, rnd);
  bool err = false;
  double stime = tctime();
  TCHDB *hdb = tchdbnew();
  if(g_dbgfd >= 0) tchdbsetdbgfd(hdb, g_dbgfd);
  if(mt && !tchdbsetmutex(hdb)){
    eprint(hdb, "tchdbsetmutex");
    err = true;
  }
  if(!tchdbsetcodecfunc(hdb, _tc_recencode, NULL, _tc_recdecode, NULL)){
    eprint(hdb, "tchdbsetcodecfunc");
    err = true;
  }
  if(!tchdbsetcache(hdb, rcnum)){
    eprint(hdb, "tchdbsetcache");
    err = true;
  }
  if(xmsiz >= 0 && !tchdbsetxmsiz(hdb, xmsiz)){
    eprint(hdb, "tchdbsetxmsiz");
    err = true;
  }
  if(!tchdbopen(hdb, path, HDBOWRITER | omode)){
    eprint(hdb, "tchdbopen");
    err = true;
  }
  int rnum = tchdbrnum(hdb);
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%08d", rnd ? myrand(rnum) + 1 : i);
    if(!tchdbout(hdb, kbuf, ksiz) && !(rnd && tchdbecode(hdb) == TCENOREC)){
      eprint(hdb, "tchdbout");
      err = true;
      break;
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tchdbrnum(hdb));
  iprintf("size: %llu\n", (unsigned long long)tchdbfsiz(hdb));
  mprint(hdb);
  if(!tchdbclose(hdb)){
    eprint(hdb, "tchdbclose");
    err = true;
  }
  tchdbdel(hdb);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform rcat command */
static int procrcat(const char *path, int rnum, int bnum, int apow, int fpow,
                    bool mt, int opts, int rcnum, int xmsiz, int omode, int pnum,
                    bool dai, bool dad, bool rl){
  iprintf("<Random Concatenating Test>\n"
          "  path=%s  rnum=%d  bnum=%d  apow=%d  fpow=%d  mt=%d  opts=%d  rcnum=%d  xmsiz=%d"
          "  omode=%d  pnum=%d  dai=%d  dad=%d  rl=%d\n\n",
          path, rnum, bnum, apow, fpow, mt, opts, rcnum, xmsiz, omode, pnum, dai, dad, rl);
  if(pnum < 1) pnum = rnum;
  bool err = false;
  double stime = tctime();
  TCHDB *hdb = tchdbnew();
  if(g_dbgfd >= 0) tchdbsetdbgfd(hdb, g_dbgfd);
  if(mt && !tchdbsetmutex(hdb)){
    eprint(hdb, "tchdbsetmutex");
    err = true;
  }
  if(!tchdbsetcodecfunc(hdb, _tc_recencode, NULL, _tc_recdecode, NULL)){
    eprint(hdb, "tchdbsetcodecfunc");
    err = true;
  }
  if(!tchdbtune(hdb, bnum, apow, fpow, opts)){
    eprint(hdb, "tchdbtune");
    err = true;
  }
  if(!tchdbsetcache(hdb, rcnum)){
    eprint(hdb, "tchdbsetcache");
    err = true;
  }
  if(xmsiz >= 0 && !tchdbsetxmsiz(hdb, xmsiz)){
    eprint(hdb, "tchdbsetxmsiz");
    err = true;
  }
  if(!tchdbopen(hdb, path, HDBOWRITER | HDBOCREAT | HDBOTRUNC | omode)){
    eprint(hdb, "tchdbopen");
    err = true;
  }
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", myrand(pnum));
    if(dai){
      if(tchdbaddint(hdb, kbuf, ksiz, myrand(3)) == INT_MIN){
        eprint(hdb, "tchdbaddint");
        err = true;
        break;
      }
    } else if(dad){
      if(isnan(tchdbadddouble(hdb, kbuf, ksiz, myrand(3)))){
        eprint(hdb, "tchdbadddouble");
        err = true;
        break;
      }
    } else if(rl){
      char vbuf[PATH_MAX];
      int vsiz = myrand(PATH_MAX);
      for(int j = 0; j < vsiz; j++){
        vbuf[j] = myrand(0x100);
      }
      if(!tchdbputcat(hdb, kbuf, ksiz, vbuf, vsiz)){
        eprint(hdb, "tchdbputcat");
        err = true;
        break;
      }
    } else {
      if(!tchdbputcat(hdb, kbuf, ksiz, kbuf, ksiz)){
        eprint(hdb, "tchdbputcat");
        err = true;
        break;
      }
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  iprintf("record number: %llu\n", (unsigned long long)tchdbrnum(hdb));
  iprintf("size: %llu\n", (unsigned long long)tchdbfsiz(hdb));
  mprint(hdb);
  if(!tchdbclose(hdb)){
    eprint(hdb, "tchdbclose");
    err = true;
  }
  tchdbdel(hdb);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform misc command */
static int procmisc(const char *path, int rnum, bool mt, int opts, int omode){
  iprintf("<Miscellaneous Test>\n  path=%s  rnum=%d  mt=%d  opts=%d  omode=%d\n\n",
          path, rnum, mt, opts, omode);
  bool err = false;
  double stime = tctime();
  TCHDB *hdb = tchdbnew();
  if(g_dbgfd >= 0) tchdbsetdbgfd(hdb, g_dbgfd);
  if(mt && !tchdbsetmutex(hdb)){
    eprint(hdb, "tchdbsetmutex");
    err = true;
  }
  if(!tchdbsetcodecfunc(hdb, _tc_recencode, NULL, _tc_recdecode, NULL)){
    eprint(hdb, "tchdbsetcodecfunc");
    err = true;
  }
  if(!tchdbtune(hdb, rnum / 50, 2, -1, opts)){
    eprint(hdb, "tchdbtune");
    err = true;
  }
  if(!tchdbsetcache(hdb, rnum / 10)){
    eprint(hdb, "tchdbsetcache");
    err = true;
  }
  if(!tchdbsetxmsiz(hdb, rnum * sizeof(int))){
    eprint(hdb, "tchdbsetxmsiz");
    err = true;
  }
  if(!tchdbopen(hdb, path, HDBOWRITER | HDBOCREAT | HDBOTRUNC | omode)){
    eprint(hdb, "tchdbopen");
    err = true;
  }
  iprintf("writing:\n");
  for(int i = 1; i <= rnum; i++){
    char buf[RECBUFSIZ];
    int len = sprintf(buf, "%08d", i);
    if(i % 3 == 0){
      if(!tchdbputkeep(hdb, buf, len, buf, len)){
        eprint(hdb, "tchdbputkeep");
        err = true;
        break;
      }
    } else {
      if(!tchdbputasync(hdb, buf, len, buf, len)){
        eprint(hdb, "tchdbputasync");
        err = true;
        break;
      }
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
    char *vbuf = tchdbget(hdb, kbuf, ksiz, &vsiz);
    if(!vbuf){
      eprint(hdb, "tchdbget");
      err = true;
      break;
    } else if(vsiz != ksiz || memcmp(vbuf, kbuf, vsiz)){
      eprint(hdb, "(validation)");
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
  if(tchdbrnum(hdb) != rnum){
    eprint(hdb, "(validation)");
    err = true;
  }
  iprintf("random writing:\n");
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", myrand(rnum));
    char vbuf[RECBUFSIZ];
    int vsiz = myrand(RECBUFSIZ);
    memset(vbuf, '*', vsiz);
    if(!tchdbput(hdb, kbuf, ksiz, vbuf, vsiz)){
      eprint(hdb, "tchdbput");
      err = true;
      break;
    }
    int rsiz;
    char *rbuf = tchdbget(hdb, kbuf, ksiz, &rsiz);
    if(!rbuf){
      eprint(hdb, "tchdbget");
      err = true;
      break;
    }
    if(rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
      eprint(hdb, "(validation)");
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
    if(!tchdbputkeep(hdb, kbuf, ksiz, vbuf, vsiz)){
      eprint(hdb, "tchdbputkeep");
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
    if(!tchdbout(hdb, kbuf, ksiz) && tchdbecode(hdb) != TCENOREC){
      eprint(hdb, "tchdbout");
      err = true;
      break;
    }
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
    if(!tchdbputkeep(hdb, kbuf, ksiz, vbuf, vsiz)){
      eprint(hdb, "tchdbputkeep");
      err = true;
      break;
    }
    if(vsiz < 1){
      char tbuf[PATH_MAX];
      for(int j = 0; j < PATH_MAX; j++){
        tbuf[j] = myrand(0x100);
      }
      if(!tchdbput(hdb, kbuf, ksiz, tbuf, PATH_MAX)){
        eprint(hdb, "tchdbput");
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
      if(!tchdbout(hdb, kbuf, ksiz)){
        eprint(hdb, "tchdbout");
        err = true;
        break;
      }
      if(tchdbout(hdb, kbuf, ksiz) || tchdbecode(hdb) != TCENOREC){
        eprint(hdb, "tchdbout");
        err = true;
        break;
      }
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  iprintf("random writing and reopening:\n");
  for(int i = 1; i <= rnum; i++){
    if(myrand(10) == 0){
      int ksiz, vsiz;
      char *kbuf, *vbuf;
      ksiz = (myrand(5) == 0) ? myrand(UINT16_MAX) : myrand(RECBUFSIZ);
      kbuf = tcmalloc(ksiz + 1);
      memset(kbuf, '@', ksiz);
      vsiz = (myrand(5) == 0) ? myrand(UINT16_MAX) : myrand(RECBUFSIZ);
      vbuf = tcmalloc(vsiz + 1);
      for(int j = 0; j < vsiz; j++){
        vbuf[j] = myrand(256);
      }
      switch(myrand(4)){
      case 0:
        if(!tchdbput(hdb, kbuf, ksiz, vbuf, vsiz)){
          eprint(hdb, "tchdbput");
          err = true;
        }
        break;
      case 1:
        if(!tchdbputcat(hdb, kbuf, ksiz, vbuf, vsiz)){
          eprint(hdb, "tchdbputcat");
          err = true;
        }
        break;
      case 2:
        if(!tchdbputasync(hdb, kbuf, ksiz, vbuf, vsiz)){
          eprint(hdb, "tchdbputasync");
          err = true;
        }
        break;
      case 3:
        if(!tchdbout(hdb, kbuf, ksiz) && tchdbecode(hdb) != TCENOREC){
          eprint(hdb, "tchdbout");
          err = true;
        }
        break;
      }
      tcfree(vbuf);
      tcfree(kbuf);
    } else {
      char kbuf[RECBUFSIZ];
      int ksiz = myrand(RECBUFSIZ);
      memset(kbuf, '@', ksiz);
      char vbuf[RECBUFSIZ];
      int vsiz = myrand(RECBUFSIZ);
      memset(vbuf, '@', vsiz);
      if(!tchdbput(hdb, kbuf, ksiz, vbuf, vsiz)){
        eprint(hdb, "tchdbputcat");
        err = true;
        break;
      }
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  if(!tchdbclose(hdb)){
    eprint(hdb, "tchdbclose");
    err = true;
  }
  if(!tchdbopen(hdb, path, HDBOWRITER | omode)){
    eprint(hdb, "tchdbopen");
    err = true;
  }
  iprintf("checking:\n");
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "[%d]", i);
    int vsiz;
    char *vbuf = tchdbget(hdb, kbuf, ksiz, &vsiz);
    if(i % 2 == 0){
      if(!vbuf){
        eprint(hdb, "tchdbget");
        err = true;
        break;
      }
      if(vsiz != i % RECBUFSIZ && vsiz != PATH_MAX){
        eprint(hdb, "(validation)");
        err = true;
        tcfree(vbuf);
        break;
      }
    } else {
      if(vbuf || tchdbecode(hdb) != TCENOREC){
        eprint(hdb, "(validation)");
        err = true;
        tcfree(vbuf);
        break;
      }
    }
    tcfree(vbuf);
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  iprintf("writing:\n");
  for(int i = 1; i <= rnum; i++){
    char buf[RECBUFSIZ];
    int len = sprintf(buf, "%08d", i);
    if(!tchdbput(hdb, buf, len, buf, len)){
      eprint(hdb, "tchdbput");
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
    char *vbuf = tchdbget(hdb, kbuf, ksiz, &vsiz);
    if(!vbuf){
      eprint(hdb, "tchdbget");
      err = true;
      break;
    } else if(vsiz != ksiz || memcmp(vbuf, kbuf, vsiz)){
      eprint(hdb, "(validation)");
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
  iprintf("checking words:\n");
  for(int i = 0; words[i] != NULL; i += 2){
    const char *kbuf = words[i];
    int ksiz = strlen(kbuf);
    const char *vbuf = words[i+1];
    int vsiz = strlen(vbuf);
    int rsiz;
    char *rbuf = tchdbget(hdb, kbuf, ksiz, &rsiz);
    if(!rbuf){
      eprint(hdb, "tchdbget");
      err = true;
      break;
    } else if(rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
      eprint(hdb, "(validation)");
      err = true;
      tcfree(rbuf);
      break;
    }
    tcfree(rbuf);
    if(rnum > 250) iputchar('.');
  }
  if(rnum > 250) iprintf(" (%08d)\n", sizeof(words) / sizeof(*words));
  iprintf("checking iterator:\n");
  int inum = 0;
  if(!tchdbiterinit(hdb)){
    eprint(hdb, "tchdbiterinit");
    err = true;
  }
  char *kbuf;
  int ksiz;
  for(int i = 1; (kbuf = tchdbiternext(hdb, &ksiz)) != NULL; i++, inum++){
    int vsiz;
    char *vbuf = tchdbget(hdb, kbuf, ksiz, &vsiz);
    if(!vbuf){
      eprint(hdb, "tchdbget");
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
  if(tchdbecode(hdb) != TCENOREC || inum != tchdbrnum(hdb)){
    eprint(hdb, "(validation)");
    err = true;
  }
  iprintf("iteration updating:\n");
  if(!tchdbiterinit(hdb)){
    eprint(hdb, "tchdbiterinit");
    err = true;
  }
  inum = 0;
  for(int i = 1; (kbuf = tchdbiternext(hdb, &ksiz)) != NULL; i++, inum++){
    if(myrand(2) == 0){
      if(!tchdbputcat(hdb, kbuf, ksiz, "0123456789", 10)){
        eprint(hdb, "tchdbputcat");
        err = true;
        tcfree(kbuf);
        break;
      }
    } else {
      if(!tchdbout(hdb, kbuf, ksiz)){
        eprint(hdb, "tchdbout");
        err = true;
        tcfree(kbuf);
        break;
      }
    }
    tcfree(kbuf);
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  if(rnum > 250) iprintf(" (%08d)\n", inum);
  if(tchdbecode(hdb) != TCENOREC || inum < tchdbrnum(hdb)){
    eprint(hdb, "(validation)");
    err = true;
  }
  if(!tchdbsync(hdb)){
    eprint(hdb, "tchdbsync");
    err = true;
  }
  if(!tchdbvanish(hdb)){
    eprint(hdb, "tchdbvanish");
    err = true;
  }
  TCMAP *map = tcmapnew();
  iprintf("random writing:\n");
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", myrand(rnum));
    char vbuf[RECBUFSIZ];
    int vsiz = sprintf(vbuf, "%d", myrand(rnum));
    switch(myrand(4)){
    case 0:
      if(!tchdbput(hdb, kbuf, ksiz, vbuf, vsiz)){
        eprint(hdb, "tchdbput");
        err = true;
      }
      tcmapput(map, kbuf, ksiz, vbuf, vsiz);
      break;
    case 1:
      if(!tchdbputkeep(hdb, kbuf, ksiz, vbuf, vsiz) && tchdbecode(hdb) != TCEKEEP){
        eprint(hdb, "tchdbputkeep");
        err = true;
      }
      tcmapputkeep(map, kbuf, ksiz, vbuf, vsiz);
      break;
    case 2:
      if(!tchdbputcat(hdb, kbuf, ksiz, vbuf, vsiz)){
        eprint(hdb, "tchdbputcat");
        err = true;
      }
      tcmapputcat(map, kbuf, ksiz, vbuf, vsiz);
      break;
    case 3:
      if(!tchdbout(hdb, kbuf, ksiz) && tchdbecode(hdb) != TCENOREC){
        eprint(hdb, "tchdbout");
        err = true;
      }
      tcmapout(map, kbuf, ksiz);
      break;
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  iprintf("checking transaction commit:\n");
  if(!tchdbtranbegin(hdb)){
    eprint(hdb, "tchdbtranbegin");
    err = true;
  }
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", myrand(rnum));
    char vbuf[RECBUFSIZ];
    int vsiz = sprintf(vbuf, "[%d]", myrand(rnum));
    switch(myrand(4)){
    case 0:
      if(!tchdbput(hdb, kbuf, ksiz, vbuf, vsiz)){
        eprint(hdb, "tchdbput");
        err = true;
      }
      tcmapput(map, kbuf, ksiz, vbuf, vsiz);
      break;
    case 1:
      if(!tchdbputkeep(hdb, kbuf, ksiz, vbuf, vsiz) && tchdbecode(hdb) != TCEKEEP){
        eprint(hdb, "tchdbputkeep");
        err = true;
      }
      tcmapputkeep(map, kbuf, ksiz, vbuf, vsiz);
      break;
    case 2:
      if(!tchdbputcat(hdb, kbuf, ksiz, vbuf, vsiz)){
        eprint(hdb, "tchdbputcat");
        err = true;
      }
      tcmapputcat(map, kbuf, ksiz, vbuf, vsiz);
      break;
    case 3:
      if(!tchdbout(hdb, kbuf, ksiz) && tchdbecode(hdb) != TCENOREC){
        eprint(hdb, "tchdbout");
        err = true;
      }
      tcmapout(map, kbuf, ksiz);
      break;
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  if(!tchdbtrancommit(hdb)){
    eprint(hdb, "tchdbtrancommit");
    err = true;
  }
  iprintf("checking transaction abort:\n");
  uint64_t ornum = tchdbrnum(hdb);
  uint64_t ofsiz = tchdbfsiz(hdb);
  if(!tchdbtranbegin(hdb)){
    eprint(hdb, "tchdbtranbegin");
    err = true;
  }
  for(int i = 1; i <= rnum; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", myrand(rnum));
    char vbuf[RECBUFSIZ];
    int vsiz = sprintf(vbuf, "((%d))", myrand(rnum));
    switch(myrand(4)){
    case 0:
      if(!tchdbput(hdb, kbuf, ksiz, vbuf, vsiz)){
        eprint(hdb, "tchdbput");
        err = true;
      }
      break;
    case 1:
      if(!tchdbputkeep(hdb, kbuf, ksiz, vbuf, vsiz) && tchdbecode(hdb) != TCEKEEP){
        eprint(hdb, "tchdbputkeep");
        err = true;
      }
      break;
    case 2:
      if(!tchdbputcat(hdb, kbuf, ksiz, vbuf, vsiz)){
        eprint(hdb, "tchdbputcat");
        err = true;
      }
      break;
    case 3:
      if(!tchdbout(hdb, kbuf, ksiz) && tchdbecode(hdb) != TCENOREC){
        eprint(hdb, "tchdbout");
        err = true;
      }
      break;
    }
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  if(!tchdbtranabort(hdb)){
    eprint(hdb, "tchdbtranabort");
    err = true;
  }
  iprintf("checking consistency:\n");
  if(tchdbrnum(hdb) != ornum || tchdbfsiz(hdb) != ofsiz || tchdbrnum(hdb) != tcmaprnum(map)){
    eprint(hdb, "(validation)");
    err = true;
  }
  inum = 0;
  tcmapiterinit(map);
  const char *tkbuf;
  int tksiz;
  for(int i = 1; (tkbuf = tcmapiternext(map, &tksiz)) != NULL; i++, inum++){
    int tvsiz;
    const char *tvbuf = tcmapiterval(tkbuf, &tvsiz);
    int rsiz;
    char *rbuf = tchdbget(hdb, tkbuf, tksiz, &rsiz);
    if(!rbuf || rsiz != tvsiz || memcmp(rbuf, tvbuf, rsiz)){
      eprint(hdb, "(validation)");
      err = true;
      tcfree(rbuf);
      break;
    }
    tcfree(rbuf);
    if(rnum > 250 && i % (rnum / 250) == 0){
      iputchar('.');
      if(i == rnum || i % (rnum / 10) == 0) iprintf(" (%08d)\n", i);
    }
  }
  if(rnum > 250) iprintf(" (%08d)\n", inum);
  inum = 0;
  if(!tchdbiterinit(hdb)){
    eprint(hdb, "tchdbiterinit");
    err = true;
  }
  for(int i = 1; (kbuf = tchdbiternext(hdb, &ksiz)) != NULL; i++, inum++){
    int vsiz;
    char *vbuf = tchdbget(hdb, kbuf, ksiz, &vsiz);
    int rsiz;
    const char *rbuf = tcmapget(map, kbuf, ksiz, &rsiz);
    if(!rbuf || rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
      eprint(hdb, "(validation)");
      err = true;
      tcfree(vbuf);
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
  tcmapdel(map);
  if(!tchdbvanish(hdb)){
    eprint(hdb, "tchdbvanish");
    err = true;
  }
  for(int i = myrand(3) + 1; i < PATH_MAX; i = i * 2 + myrand(3)){
    char vbuf[i];
    memset(vbuf, '@', i - 1);
    vbuf[i-1] = '\0';
    if(!tchdbput2(hdb, "mikio", vbuf)){
      eprint(hdb, "tchdbput2");
      err = true;
    }
  }
  if(!tchdbput2(hdb, "mikio", "nanashi")){
    eprint(hdb, "tchdbput2");
    err = true;
  }
  if(!tchdbtranbegin(hdb)){
    eprint(hdb, "tchdbtranbegin");
    err = true;
  }
  if(!tchdbput2(hdb, "mikio", "hirabayashi")){
    eprint(hdb, "tchdbput2");
    err = true;
  }
  for(int i = 0; i < 10; i++){
    char buf[RECBUFSIZ];
    int size = sprintf(buf, "%d", myrand(rnum));
    if(!tchdbput(hdb, buf, size, buf, size)){
      eprint(hdb, "tchdbput");
      err = true;
    }
  }
  for(int i = myrand(3) + 1; i < PATH_MAX; i = i * 2 + myrand(3)){
    char vbuf[i];
    memset(vbuf, '@', i - 1);
    vbuf[i-1] = '\0';
    if(!tchdbput2(hdb, "mikio", vbuf)){
      eprint(hdb, "tchdbput2");
      err = true;
    }
  }
  if(!tchdbforeach(hdb, iterfunc, NULL)){
    eprint(hdb, "tchdbforeach");
    err = true;
  }
  iprintf("record number: %llu\n", (unsigned long long)tchdbrnum(hdb));
  iprintf("size: %llu\n", (unsigned long long)tchdbfsiz(hdb));
  mprint(hdb);
  if(!tchdbclose(hdb)){
    eprint(hdb, "tchdbclose");
    err = true;
  }
  tchdbdel(hdb);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


/* perform wicked command */
static int procwicked(const char *path, int rnum, bool mt, int opts, int omode){
  iprintf("<Wicked Writing Test>\n  path=%s  rnum=%d  mt=%d  opts=%d  omode=%d\n\n",
          path, rnum, mt, opts, omode);
  bool err = false;
  double stime = tctime();
  TCHDB *hdb = tchdbnew();
  if(g_dbgfd >= 0) tchdbsetdbgfd(hdb, g_dbgfd);
  if(mt && !tchdbsetmutex(hdb)){
    eprint(hdb, "tchdbsetmutex");
    err = true;
  }
  if(!tchdbsetcodecfunc(hdb, _tc_recencode, NULL, _tc_recdecode, NULL)){
    eprint(hdb, "tchdbsetcodecfunc");
    err = true;
  }
  if(!tchdbtune(hdb, rnum / 50, 2, -1, opts)){
    eprint(hdb, "tchdbtune");
    err = true;
  }
  if(!tchdbsetcache(hdb, rnum / 10)){
    eprint(hdb, "tchdbsetcache");
    err = true;
  }
  if(!tchdbsetxmsiz(hdb, rnum * sizeof(int))){
    eprint(hdb, "tchdbsetxmsiz");
    err = true;
  }
  if(!tchdbopen(hdb, path, HDBOWRITER | HDBOCREAT | HDBOTRUNC | omode)){
    eprint(hdb, "tchdbopen");
    err = true;
  }
  if(!tchdbiterinit(hdb)){
    eprint(hdb, "tchdbiterinit");
    err = true;
  }
  TCMAP *map = tcmapnew2(rnum / 5);
  for(int i = 1; i <= rnum && !err; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", myrand(rnum));
    char vbuf[RECBUFSIZ];
    int vsiz = myrand(RECBUFSIZ);
    memset(vbuf, '*', vsiz);
    vbuf[vsiz] = '\0';
    char *rbuf;
    switch(myrand(16)){
    case 0:
      iputchar('0');
      if(!tchdbput(hdb, kbuf, ksiz, vbuf, vsiz)){
        eprint(hdb, "tchdbput");
        err = true;
      }
      tcmapput(map, kbuf, ksiz, vbuf, vsiz);
      break;
    case 1:
      iputchar('1');
      if(!tchdbput2(hdb, kbuf, vbuf)){
        eprint(hdb, "tchdbput2");
        err = true;
      }
      tcmapput2(map, kbuf, vbuf);
      break;
    case 2:
      iputchar('2');
      if(!tchdbputkeep(hdb, kbuf, ksiz, vbuf, vsiz) && tchdbecode(hdb) != TCEKEEP){
        eprint(hdb, "tchdbputkeep");
        err = true;
      }
      tcmapputkeep(map, kbuf, ksiz, vbuf, vsiz);
      break;
    case 3:
      iputchar('3');
      if(!tchdbputkeep2(hdb, kbuf, vbuf) && tchdbecode(hdb) != TCEKEEP){
        eprint(hdb, "tchdbputkeep2");
        err = true;
      }
      tcmapputkeep2(map, kbuf, vbuf);
      break;
    case 4:
      iputchar('4');
      if(!tchdbputcat(hdb, kbuf, ksiz, vbuf, vsiz)){
        eprint(hdb, "tchdbputcat");
        err = true;
      }
      tcmapputcat(map, kbuf, ksiz, vbuf, vsiz);
      break;
    case 5:
      iputchar('5');
      if(!tchdbputcat2(hdb, kbuf, vbuf)){
        eprint(hdb, "tchdbputcat2");
        err = true;
      }
      tcmapputcat2(map, kbuf, vbuf);
      break;
    case 6:
      iputchar('6');
      if(!tchdbputasync(hdb, kbuf, ksiz, vbuf, vsiz)){
        eprint(hdb, "tchdbputasync");
        err = true;
      }
      tcmapput(map, kbuf, ksiz, vbuf, vsiz);
      break;
    case 7:
      iputchar('7');
      if(!tchdbputasync2(hdb, kbuf, vbuf)){
        eprint(hdb, "tchdbputasync2");
        err = true;
      }
      tcmapput2(map, kbuf, vbuf);
      break;
    case 8:
      iputchar('8');
      if(myrand(10) == 0){
        if(!tchdbout(hdb, kbuf, ksiz) && tchdbecode(hdb) != TCENOREC){
          eprint(hdb, "tchdbout");
          err = true;
        }
        tcmapout(map, kbuf, ksiz);
      }
      break;
    case 9:
      iputchar('9');
      if(myrand(10) == 0){
        if(!tchdbout2(hdb, kbuf) && tchdbecode(hdb) != TCENOREC){
          eprint(hdb, "tchdbout2");
          err = true;
        }
        tcmapout2(map, kbuf);
      }
      break;
    case 10:
      iputchar('A');
      if(!(rbuf = tchdbget(hdb, kbuf, ksiz, &vsiz))){
        if(tchdbecode(hdb) != TCENOREC){
          eprint(hdb, "tchdbget");
          err = true;
        }
        rbuf = tcsprintf("[%d]", myrand(i + 1));
        vsiz = strlen(rbuf);
      }
      vsiz += myrand(vsiz);
      if(myrand(3) == 0) vsiz += PATH_MAX;
      rbuf = tcrealloc(rbuf, vsiz + 1);
      for(int j = 0; j < vsiz; j++){
        rbuf[j] = myrand(0x100);
      }
      if(!tchdbput(hdb, kbuf, ksiz, rbuf, vsiz)){
        eprint(hdb, "tchdbput");
        err = true;
      }
      tcmapput(map, kbuf, ksiz, rbuf, vsiz);
      tcfree(rbuf);
      break;
    case 11:
      iputchar('B');
      if(!(rbuf = tchdbget(hdb, kbuf, ksiz, &vsiz)) && tchdbecode(hdb) != TCENOREC){
        eprint(hdb, "tchdbget");
        err = true;
      }
      tcfree(rbuf);
      break;
    case 12:
      iputchar('C');
      if(!(rbuf = tchdbget2(hdb, kbuf)) && tchdbecode(hdb) != TCENOREC){
        eprint(hdb, "tchdbget2");
        err = true;
      }
      tcfree(rbuf);
      break;
    case 13:
      iputchar('D');
      if(myrand(1) == 0) vsiz = 1;
      if((vsiz = tchdbget3(hdb, kbuf, ksiz, vbuf, vsiz)) < 0 && tchdbecode(hdb) != TCENOREC){
        eprint(hdb, "tchdbget3");
        err = true;
      }
      break;
    case 14:
      iputchar('E');
      if(myrand(rnum / 50) == 0){
        if(!tchdbiterinit(hdb)){
          eprint(hdb, "tchdbiterinit");
          err = true;
        }
      }
      TCXSTR *ikey = tcxstrnew();
      TCXSTR *ival = tcxstrnew();
      for(int j = myrand(rnum) / 1000 + 1; j >= 0; j--){
        if(j % 3 == 0){
          if(tchdbiternext3(hdb, ikey, ival)){
            if(tcxstrsize(ival) != tchdbvsiz(hdb, tcxstrptr(ikey), tcxstrsize(ikey))){
              eprint(hdb, "(validation)");
              err = true;
            }
          } else {
            int ecode = tchdbecode(hdb);
            if(ecode != TCEINVALID && ecode != TCENOREC){
              eprint(hdb, "tchdbiternext3");
              err = true;
            }
          }
        } else {
          int iksiz;
          char *ikbuf = tchdbiternext(hdb, &iksiz);
          if(ikbuf){
            tcfree(ikbuf);
          } else {
            int ecode = tchdbecode(hdb);
            if(ecode != TCEINVALID && ecode != TCENOREC){
              eprint(hdb, "tchdbiternext");
              err = true;
            }
          }
        }
      }
      tcxstrdel(ival);
      tcxstrdel(ikey);
      break;
    default:
      iputchar('@');
      if(myrand(10000) == 0) srand((unsigned int)(tctime() * 1000) % UINT_MAX);
      if(myrand(rnum / 16 + 1) == 0){
        int cnt = myrand(30);
        for(int j = 0; j < rnum && !err; j++){
          ksiz = sprintf(kbuf, "%d", i + j);
          if(tchdbout(hdb, kbuf, ksiz)){
            cnt--;
          } else if(tchdbecode(hdb) != TCENOREC){
            eprint(hdb, "tcbdbout");
            err = true;
          }
          tcmapout(map, kbuf, ksiz);
          if(cnt < 0) break;
        }
      }
      break;
    }
    if(i % 50 == 0) iprintf(" (%08d)\n", i);
    if(i == rnum / 2){
      if(!tchdbclose(hdb)){
        eprint(hdb, "tchdbclose");
        err = true;
      }
      if(!tchdbopen(hdb, path, HDBOWRITER | omode)){
        eprint(hdb, "tchdbopen");
        err = true;
      }
    } else if(i == rnum / 4){
      char *npath = tcsprintf("%s-tmp", path);
      if(!tchdbcopy(hdb, npath)){
        eprint(hdb, "tchdbcopy");
        err = true;
      }
      TCHDB *nhdb = tchdbnew();
      if(!tchdbsetcodecfunc(nhdb, _tc_recencode, NULL, _tc_recdecode, NULL)){
        eprint(nhdb, "tchdbsetcodecfunc");
        err = true;
      }
      if(!tchdbopen(nhdb, npath, HDBOREADER | omode)){
        eprint(nhdb, "tchdbopen");
        err = true;
      }
      tchdbdel(nhdb);
      unlink(npath);
      tcfree(npath);
      if(!tchdboptimize(hdb, rnum / 50, -1, -1, -1)){
        eprint(hdb, "tchdboptimize");
        err = true;
      }
      if(!tchdbiterinit(hdb)){
        eprint(hdb, "tchdbiterinit");
        err = true;
      }
    } else if(i == rnum / 8){
      if(!tchdbtranbegin(hdb)){
        eprint(hdb, "tchdbtranbegin");
        err = true;
      }
    } else if(i == rnum / 8 + rnum / 16){
      if(!tchdbtrancommit(hdb)){
        eprint(hdb, "tchdbtrancommit");
        err = true;
      }
    }
  }
  if(rnum % 50 > 0) iprintf(" (%08d)\n", rnum);
  if(!tchdbsync(hdb)){
    eprint(hdb, "tchdbsync");
    err = true;
  }
  if(tchdbrnum(hdb) != tcmaprnum(map)){
    eprint(hdb, "(validation)");
    err = true;
  }
  for(int i = 1; i <= rnum && !err; i++){
    char kbuf[RECBUFSIZ];
    int ksiz = sprintf(kbuf, "%d", i - 1);
    int vsiz;
    const char *vbuf = tcmapget(map, kbuf, ksiz, &vsiz);
    int rsiz;
    char *rbuf = tchdbget(hdb, kbuf, ksiz, &rsiz);
    if(vbuf){
      iputchar('.');
      if(!rbuf){
        eprint(hdb, "tchdbget");
        err = true;
      } else if(rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
        eprint(hdb, "(validation)");
        err = true;
      }
    } else {
      iputchar('*');
      if(rbuf || tchdbecode(hdb) != TCENOREC){
        eprint(hdb, "(validation)");
        err = true;
      }
    }
    tcfree(rbuf);
    if(i % 50 == 0) iprintf(" (%08d)\n", i);
  }
  if(rnum % 50 > 0) iprintf(" (%08d)\n", rnum);
  int inum = 0;
  int csiz;
  char *cbuf = tchdbgetnext(hdb, NULL, -1, &csiz);
  while(cbuf){
    inum++;
    iputchar(':');
    int nsiz;
    char *nbuf = tchdbgetnext(hdb, cbuf, csiz, &nsiz);
    tcfree(cbuf);
    cbuf = nbuf;
    csiz = nsiz;
    if(inum % 50 == 0) iprintf(" (%08d)\n", inum);
  }
  tcfree(cbuf);
  if(inum % 50 > 0) iprintf(" (%08d)\n", inum);
  if(inum != tchdbrnum(hdb)){
    eprint(hdb, "(validation)");
    err = true;
  }
  tcmapiterinit(map);
  int ksiz;
  const char *kbuf;
  for(int i = 1; (kbuf = tcmapiternext(map, &ksiz)) != NULL; i++){
    iputchar('+');
    int vsiz;
    const char *vbuf = tcmapiterval(kbuf, &vsiz);
    int rsiz;
    char *rbuf = tchdbget(hdb, kbuf, ksiz, &rsiz);
    if(!rbuf){
      eprint(hdb, "tchdbget");
      err = true;
    } else if(rsiz != vsiz || memcmp(rbuf, vbuf, rsiz)){
      eprint(hdb, "(validation)");
      err = true;
    }
    tcfree(rbuf);
    if(!tchdbout(hdb, kbuf, ksiz)){
      eprint(hdb, "tchdbout");
      err = true;
    }
    if(i % 50 == 0) iprintf(" (%08d)\n", i);
  }
  int mrnum = tcmaprnum(map);
  if(mrnum % 50 > 0) iprintf(" (%08d)\n", mrnum);
  if(tchdbrnum(hdb) != 0){
    eprint(hdb, "(validation)");
    err = true;
  }
  iprintf("record number: %llu\n", (unsigned long long)tchdbrnum(hdb));
  iprintf("size: %llu\n", (unsigned long long)tchdbfsiz(hdb));
  mprint(hdb);
  tcmapdel(map);
  if(!tchdbclose(hdb)){
    eprint(hdb, "tchdbclose");
    err = true;
  }
  tchdbdel(hdb);
  iprintf("time: %.3f\n", tctime() - stime);
  iprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}



// END OF FILE
