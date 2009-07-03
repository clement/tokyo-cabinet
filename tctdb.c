/*************************************************************************************************
 * The table database API of Tokyo Cabinet
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


#include "tcutil.h"
#include "tchdb.h"
#include "tcbdb.h"
#include "tctdb.h"
#include "myconf.h"

#define TDBOPAQUESIZ   64                // size of using opaque field
#define TDBLEFTOPQSIZ  64                // size of left opaque field
#define TDBPAGEBUFSIZ  32768             // size of a buffer to read each page

#define TDBDEFBNUM     131071            // default bucket number
#define TDBDEFAPOW     4                 // default alignment power
#define TDBDEFFPOW     10                // default free block pool power
#define TDBDEFLCNUM    4096              // default number of leaf cache
#define TDBDEFNCNUM    512               // default number of node cache
#define TDBDEFXMSIZ    ((1LL<<20)*64)    // default size of the extra mapped memory
#define TDBIDXSUFFIX   "idx"             // suffix of column index file
#define TDBIDXLMEMB    64                // number of members in each leaf of the index
#define TDBIDXNMEMB    256               // number of members in each node of the index
#define TDBIDXLSMAX    8192              // maximum size of each leaf of the index
#define TDBIDXCCBNUM   65521             // bucket number of the index cache
#define TDBIDXCCMAX    ((1LL<<20)*64)    // maximum size of the index cache
#define TDBNUMCNTCOL   "_num"            // column name of number counting
#define TDBCOLBUFSIZ   1024              // size of a buffer for a column value
#define TDBHINTUSIZ    256               // unit size of the hint string
#define TDBORDRATIO    0.2               // ratio of records to use the order index

enum {                                   // enumeration for duplication behavior
  TDBPDOVER,                             // overwrite an existing value
  TDBPDKEEP,                             // keep the existing value
  TDBPDCAT                               // concatenate values
};

typedef struct {                         // type of structure for a sort key
  const char *kbuf;                      // pointer to the primary key
  int ksiz;                              // size of the primary key
  char *vbuf;                            // pointer to the value
  int vsiz;                              // size of the value
} TDBSORTKEY;


/* private macros */
#define TDBLOCKMETHOD(TC_tdb, TC_wr) \
  ((TC_tdb)->mmtx ? tctdblockmethod((TC_tdb), (TC_wr)) : true)
#define TDBUNLOCKMETHOD(TC_tdb) \
  ((TC_tdb)->mmtx ? tctdbunlockmethod(TC_tdb) : true)
#define TDBTHREADYIELD(TC_tdb) \
  do { if((TC_tdb)->mmtx) sched_yield(); } while(false)


/* private function prototypes */
static void tctdbclear(TCTDB *tdb);
static bool tctdbopenimpl(TCTDB *tdb, const char *path, int omode);
static bool tctdbcloseimpl(TCTDB *tdb);
static bool tctdbputimpl(TCTDB *tdb, const void *pkbuf, int pksiz, TCMAP *cols, int dmode);
static bool tctdboutimpl(TCTDB *tdb, const char *pkbuf, int pksiz);
static TCMAP *tctdbgetimpl(TCTDB *tdb, const void *pkbuf, int pksiz);
static double tctdbaddnumber(TCTDB *tdb, const void *pkbuf, int pksiz, double num);
static bool tctdboptimizeimpl(TCTDB *tdb, int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts);
static bool tctdbvanishimpl(TCTDB *tdb);
static bool tctdbcopyimpl(TCTDB *tdb, const char *path);
static bool tctdbtranbeginimpl(TCTDB *tdb);
static bool tctdbtrancommitimpl(TCTDB *tdb);
static bool tctdbtranabortimpl(TCTDB *tdb);
static bool tctdbsetindeximpl(TCTDB *tdb, const char *name, int type);
static int64_t tctdbgenuidimpl(TCTDB *tdb, int64_t inc);
static TCLIST *tctdbqrysearchimpl(TDBQRY *qry);
static TCMAP *tctdbqryidxfetch(TDBQRY *qry, TDBCOND *cond, TDBIDX *idx);
static bool tctdbqryonecondmatch(TDBQRY *qry, TDBCOND *cond, const char *pkbuf, int pksiz);
static bool tctdbqryallcondmatch(TDBQRY *qry, const char *pkbuf, int pksiz);
static bool tctdbqrycondmatch(int op, const char *expr, int esiz, const char *vbuf, int vsiz);
static bool tctdbqrycondcheckstrand(const char *tval, const char *oval);
static bool tctdbqrycondcheckstror(const char *tval, const char *oval);
static bool tctdbqrycondcheckstroreq(const char *vbuf, const char *expr);
static bool tctdbqrycondchecknumbt(const char *vbuf, const char *expr);
static bool tctdbqrycondchecknumoreq(const char *vbuf, const char *expr);
static int tdbcmppkeynumasc(const TCLISTDATUM *a, const TCLISTDATUM *b);
static int tdbcmppkeynumdesc(const TCLISTDATUM *a, const TCLISTDATUM *b);
static int tdbcmpsortkeystrasc(const TDBSORTKEY *a, const TDBSORTKEY *b);
static int tdbcmpsortkeystrdesc(const TDBSORTKEY *a, const TDBSORTKEY *b);
static int tdbcmpsortkeynumasc(const TDBSORTKEY *a, const TDBSORTKEY *b);
static int tdbcmpsortkeynumdesc(const TDBSORTKEY *a, const TDBSORTKEY *b);
static uint16_t tctdbidxhash(const char *pkbuf, int pksiz);
static bool tctdbidxput(TCTDB *tdb, const void *pkbuf, int pksiz, TCMAP *cols);
static bool tctdbidxputone(TCTDB *tdb, TDBIDX *idx, const char *pkbuf, int pksiz, uint16_t hash,
                           const char *vbuf, int vsiz);
static bool tctdbidxputtoken(TCTDB *tdb, TDBIDX *idx, const char *pkbuf, int pksiz,
                             const char *vbuf, int vsiz);
static bool tctdbidxout(TCTDB *tdb, const void *pkbuf, int pksiz, TCMAP *cols);
static bool tctdbidxoutone(TCTDB *tdb, TDBIDX *idx, const char *pkbuf, int pksiz, uint16_t hash,
                           const char *vbuf, int vsiz);
static bool tctdbidxouttoken(TCTDB *tdb, TDBIDX *idx, const char *pkbuf, int pksiz,
                             const char *vbuf, int vsiz);
static bool tctdbidxsynctoken(TCTDB *tdb, TDBIDX *idx);
static TCMAP *tctdbidxgetbytokens(TCTDB *tdb, TDBIDX *idx, const TCLIST *tokens, int op);
static bool tctdbdefragimpl(TCTDB *tdb, int64_t step);
static bool tctdbforeachimpl(TCTDB *tdb, TCITER iter, void *op);
static int tctdbqryprocoutcb(const void *pkbuf, int pksiz, TCMAP *cols, void *op);
static bool tctdblockmethod(TCTDB *tdb, bool wr);
static bool tctdbunlockmethod(TCTDB *tdb);


/* debugging function prototypes */
void tctdbprintmeta(TCTDB *tdb);



/*************************************************************************************************
 * API
 *************************************************************************************************/


/* Get the message string corresponding to an error code. */
const char *tctdberrmsg(int ecode){
  return tcerrmsg(ecode);
}


/* Create a table database object. */
TCTDB *tctdbnew(void){
  TCTDB *tdb;
  TCMALLOC(tdb, sizeof(*tdb));
  tctdbclear(tdb);
  tdb->hdb = tchdbnew();
  tchdbtune(tdb->hdb, TDBDEFBNUM, TDBDEFAPOW, TDBDEFFPOW, 0);
  tchdbsetxmsiz(tdb->hdb, TDBDEFXMSIZ);
  return tdb;
}


/* Delete a table database object. */
void tctdbdel(TCTDB *tdb){
  assert(tdb);
  if(tdb->open) tctdbclose(tdb);
  tchdbdel(tdb->hdb);
  if(tdb->mmtx){
    pthread_rwlock_destroy(tdb->mmtx);
    TCFREE(tdb->mmtx);
  }
  TCFREE(tdb);
}


/* Get the last happened error code of a table database object. */
int tctdbecode(TCTDB *tdb){
  assert(tdb);
  return tchdbecode(tdb->hdb);
}


/* Set mutual exclusion control of a table database object for threading. */
bool tctdbsetmutex(TCTDB *tdb){
  assert(tdb);
  if(!TCUSEPTHREAD) return true;
  if(tdb->mmtx || tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  TCMALLOC(tdb->mmtx, sizeof(pthread_rwlock_t));
  bool err = false;
  if(pthread_rwlock_init(tdb->mmtx, NULL) != 0) err = true;
  if(err){
    TCFREE(tdb->mmtx);
    tdb->mmtx = NULL;
    return false;
  }
  return tchdbsetmutex(tdb->hdb);
}


/* Set the tuning parameters of a table database object. */
bool tctdbtune(TCTDB *tdb, int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts){
  assert(tdb);
  if(tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  tdb->opts = opts;
  uint8_t hopts = 0;
  if(opts & TDBTLARGE) hopts |= HDBTLARGE;
  if(opts & TDBTDEFLATE) hopts |= HDBTDEFLATE;
  if(opts & TDBTBZIP) hopts |= HDBTBZIP;
  if(opts & TDBTTCBS) hopts |= HDBTTCBS;
  if(opts & TDBTEXCODEC) hopts |= HDBTEXCODEC;
  bnum = (bnum > 0) ? bnum : TDBDEFBNUM;
  apow = (apow >= 0) ? apow : TDBDEFAPOW;
  fpow = (fpow >= 0) ? fpow : TDBDEFFPOW;
  return tchdbtune(tdb->hdb, bnum, apow, fpow, hopts);
}


/* Set the caching parameters of a table database object. */
bool tctdbsetcache(TCTDB *tdb, int32_t rcnum, int32_t lcnum, int32_t ncnum){
  assert(tdb);
  if(tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  if(lcnum > 0) tdb->lcnum = lcnum;
  if(ncnum > 0) tdb->ncnum = ncnum;
  return tchdbsetcache(tdb->hdb, rcnum);
}


/* Set the size of the extra mapped memory of a table database object. */
bool tctdbsetxmsiz(TCTDB *tdb, int64_t xmsiz){
  assert(tdb);
  if(tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  return tchdbsetxmsiz(tdb->hdb, xmsiz);
}


/* Set the unit step number of auto defragmentation of a table database object. */
bool tctdbsetdfunit(TCTDB *tdb, int32_t dfunit){
  assert(tdb);
  if(tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  return tchdbsetdfunit(tdb->hdb, dfunit);
}


/* Open a database file and connect a table database object. */
bool tctdbopen(TCTDB *tdb, const char *path, int omode){
  assert(tdb && path);
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  bool rv = tctdbopenimpl(tdb, path, omode);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Close a table database object. */
bool tctdbclose(TCTDB *tdb){
  assert(tdb);
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  bool rv = tctdbcloseimpl(tdb);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Store a record into a table database object. */
bool tctdbput(TCTDB *tdb, const void *pkbuf, int pksiz, TCMAP *cols){
  assert(tdb && pkbuf && pksiz >= 0 && cols);
  int vsiz;
  if(tcmapget(cols, "", 0, &vsiz)){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(!tdb->open || !tdb->wmode){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  bool rv = tctdbputimpl(tdb, pkbuf, pksiz, cols, TDBPDOVER);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Store a string record into a table database object with a zero separated column string. */
bool tctdbput2(TCTDB *tdb, const void *pkbuf, int pksiz, const void *cbuf, int csiz){
  assert(tdb && pkbuf && pksiz >= 0 && cbuf && csiz >= 0);
  TCMAP *cols = tcstrsplit4(cbuf, csiz);
  bool rv = tctdbput(tdb, pkbuf, pksiz, cols);
  tcmapdel(cols);
  return rv;
}


/* Store a string record into a table database object with a tab separated column string. */
bool tctdbput3(TCTDB *tdb, const char *pkstr, const char *cstr){
  assert(tdb && pkstr && cstr);
  TCMAP *cols = tcstrsplit3(cstr, "\t");
  bool rv = tctdbput(tdb, pkstr, strlen(pkstr), cols);
  tcmapdel(cols);
  return rv;
}


/* Store a new record into a table database object. */
bool tctdbputkeep(TCTDB *tdb, const void *pkbuf, int pksiz, TCMAP *cols){
  assert(tdb && pkbuf && pksiz >= 0 && cols);
  int vsiz;
  if(tcmapget(cols, "", 0, &vsiz)){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(!tdb->open || !tdb->wmode){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  bool rv = tctdbputimpl(tdb, pkbuf, pksiz, cols, TDBPDKEEP);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Store a new string record into a table database object with a zero separated column string. */
bool tctdbputkeep2(TCTDB *tdb, const void *pkbuf, int pksiz, const void *cbuf, int csiz){
  assert(tdb && pkbuf && pksiz >= 0 && cbuf && csiz >= 0);
  TCMAP *cols = tcstrsplit4(cbuf, csiz);
  bool rv = tctdbputkeep(tdb, pkbuf, pksiz, cols);
  tcmapdel(cols);
  return rv;
}


/* Store a new string record into a table database object with a tab separated column string. */
bool tctdbputkeep3(TCTDB *tdb, const char *pkstr, const char *cstr){
  assert(tdb && pkstr && cstr);
  TCMAP *cols = tcstrsplit3(cstr, "\t");
  bool rv = tctdbputkeep(tdb, pkstr, strlen(pkstr), cols);
  tcmapdel(cols);
  return rv;
}


/* Concatenate columns of the existing record in a table database object. */
bool tctdbputcat(TCTDB *tdb, const void *pkbuf, int pksiz, TCMAP *cols){
  assert(tdb && pkbuf && pksiz >= 0 && cols);
  int vsiz;
  if(tcmapget(cols, "", 0, &vsiz)){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(!tdb->open || !tdb->wmode){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  bool rv = tctdbputimpl(tdb, pkbuf, pksiz, cols, TDBPDCAT);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Concatenate columns in a table database object with a zero separated column string. */
bool tctdbputcat2(TCTDB *tdb, const void *pkbuf, int pksiz, const void *cbuf, int csiz){
  assert(tdb && pkbuf && pksiz >= 0 && cbuf && csiz >= 0);
  TCMAP *cols = tcstrsplit4(cbuf, csiz);
  bool rv = tctdbputcat(tdb, pkbuf, pksiz, cols);
  tcmapdel(cols);
  return rv;
}


/* Concatenate columns in a table database object with with a tab separated column string. */
bool tctdbputcat3(TCTDB *tdb, const char *pkstr, const char *cstr){
  assert(tdb && pkstr && cstr);
  TCMAP *cols = tcstrsplit3(cstr, "\t");
  bool rv = tctdbputcat(tdb, pkstr, strlen(pkstr), cols);
  tcmapdel(cols);
  return rv;
}


/* Remove a record of a table database object. */
bool tctdbout(TCTDB *tdb, const void *pkbuf, int pksiz){
  assert(tdb && pkbuf && pksiz >= 0);
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(!tdb->open || !tdb->wmode){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  bool rv = tctdboutimpl(tdb, pkbuf, pksiz);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Remove a string record of a table database object. */
bool tctdbout2(TCTDB *tdb, const char *pkstr){
  assert(tdb && pkstr);
  return tctdbout(tdb, pkstr, strlen(pkstr));
}


/* Retrieve a record in a table database object. */
TCMAP *tctdbget(TCTDB *tdb, const void *pkbuf, int pksiz){
  assert(tdb && pkbuf && pksiz >= 0);
  if(!TDBLOCKMETHOD(tdb, false)) return NULL;
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return NULL;
  }
  TCMAP *rv = tctdbgetimpl(tdb, pkbuf, pksiz);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Retrieve a record in a table database object as a zero separated column string. */
char *tctdbget2(TCTDB *tdb, const void *pkbuf, int pksiz, int *sp){
  assert(tdb && pkbuf && pksiz >= 0 && sp);
  TCMAP *cols = tctdbget(tdb, pkbuf, pksiz);
  if(!cols) return NULL;
  char *cbuf = tcstrjoin4(cols, sp);
  tcmapdel(cols);
  return cbuf;
}


/* Retrieve a string record in a table database object as a tab separated column string. */
char *tctdbget3(TCTDB *tdb, const char *pkstr){
  assert(tdb && pkstr);
  TCMAP *cols = tctdbget(tdb, pkstr, strlen(pkstr));
  if(!cols) return NULL;
  char *cstr = tcstrjoin3(cols, '\t');
  tcmapdel(cols);
  return cstr;
}


/* Get the size of the value of a record in a table database object. */
int tctdbvsiz(TCTDB *tdb, const void *pkbuf, int pksiz){
  assert(tdb && pkbuf && pksiz >= 0);
  if(!TDBLOCKMETHOD(tdb, false)) return -1;
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return -1;
  }
  int rv = tchdbvsiz(tdb->hdb, pkbuf, pksiz);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Get the size of the value of a string record in a table database object. */
int tctdbvsiz2(TCTDB *tdb, const char *pkstr){
  assert(tdb && pkstr);
  return tctdbvsiz(tdb, pkstr, strlen(pkstr));
}


/* Initialize the iterator of a table database object. */
bool tctdbiterinit(TCTDB *tdb){
  assert(tdb);
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  bool rv = tchdbiterinit(tdb->hdb);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Get the next primary key of the iterator of a table database object. */
void *tctdbiternext(TCTDB *tdb, int *sp){
  assert(tdb && sp);
  if(!TDBLOCKMETHOD(tdb, true)) return NULL;
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return NULL;
  }
  char *rv = tchdbiternext(tdb->hdb, sp);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Get the next primary key string of the iterator of a table database object. */
char *tctdbiternext2(TCTDB *tdb){
  assert(tdb);
  int pksiz;
  return tctdbiternext(tdb, &pksiz);
}


/* Get the columns of the next record of the iterator of a table database object. */
TCMAP *tctdbiternext3(TCTDB *tdb){
  assert(tdb);
  TCXSTR *kstr = tcxstrnew();
  TCXSTR *vstr = tcxstrnew();
  TCMAP *cols = NULL;
  if(tchdbiternext3(tdb->hdb, kstr, vstr)){
    cols = tcmapload(TCXSTRPTR(vstr), TCXSTRSIZE(vstr));
    tcmapput(cols, "", 0, TCXSTRPTR(kstr), TCXSTRSIZE(kstr));
  }
  tcxstrdel(vstr);
  tcxstrdel(kstr);
  return cols;
}


/* Get forward matching primary keys in a table database object. */
TCLIST *tctdbfwmkeys(TCTDB *tdb, const void *pbuf, int psiz, int max){
  assert(tdb && pbuf && psiz >= 0);
  if(!TDBLOCKMETHOD(tdb, true)) return tclistnew();
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return tclistnew();
  }
  TCLIST *rv = tchdbfwmkeys(tdb->hdb, pbuf, psiz, max);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Get forward matching string primary keys in a table database object. */
TCLIST *tctdbfwmkeys2(TCTDB *tdb, const char *pstr, int max){
  assert(tdb && pstr);
  return tctdbfwmkeys(tdb, pstr, strlen(pstr), max);
}


/* Add an integer to a column of a record in a table database object. */
int tctdbaddint(TCTDB *tdb, const void *pkbuf, int pksiz, int num){
  assert(tdb && pkbuf && pksiz >= 0);
  if(!TDBLOCKMETHOD(tdb, true)) return INT_MIN;
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return INT_MIN;
  }
  double rv = tctdbaddnumber(tdb, pkbuf, pksiz, num);
  TDBUNLOCKMETHOD(tdb);
  return isnan(rv) ? INT_MIN : (int)rv;
}


/* Add a real number to a column of a record in a table database object. */
double tctdbadddouble(TCTDB *tdb, const void *pkbuf, int pksiz, double num){
  assert(tdb && pkbuf && pksiz >= 0);
  if(!TDBLOCKMETHOD(tdb, true)) return INT_MIN;
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return INT_MIN;
  }
  double rv = tctdbaddnumber(tdb, pkbuf, pksiz, num);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Synchronize updated contents of a table database object with the file and the device. */
bool tctdbsync(TCTDB *tdb){
  assert(tdb);
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(!tdb->open || !tdb->wmode || tdb->tran){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  bool rv = tctdbmemsync(tdb, true);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Optimize the file of a table database object. */
bool tctdboptimize(TCTDB *tdb, int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts){
  assert(tdb);
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(!tdb->open || !tdb->wmode || tdb->tran){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  TDBTHREADYIELD(tdb);
  bool rv = tctdboptimizeimpl(tdb, bnum, apow, fpow, opts);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Remove all records of a table database object. */
bool tctdbvanish(TCTDB *tdb){
  assert(tdb);
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(!tdb->open || !tdb->wmode || tdb->tran){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  TDBTHREADYIELD(tdb);
  bool rv = tctdbvanishimpl(tdb);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Copy the database file of a table database object. */
bool tctdbcopy(TCTDB *tdb, const char *path){
  assert(tdb && path);
  if(!TDBLOCKMETHOD(tdb, false)) return false;
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  TDBTHREADYIELD(tdb);
  bool rv = tctdbcopyimpl(tdb, path);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Begin the transaction of a table database object. */
bool tctdbtranbegin(TCTDB *tdb){
  assert(tdb);
  for(double wsec = 1.0 / sysconf(_SC_CLK_TCK); true; wsec *= 2){
    if(!TDBLOCKMETHOD(tdb, true)) return false;
    if(!tdb->open || !tdb->wmode){
      tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
      TDBUNLOCKMETHOD(tdb);
      return false;
    }
    if(!tdb->tran) break;
    TDBUNLOCKMETHOD(tdb);
    if(wsec > 1.0) wsec = 1.0;
    tcsleep(wsec);
  }
  if(!tctdbtranbeginimpl(tdb)){
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  tdb->tran = true;
  TDBUNLOCKMETHOD(tdb);
  return true;
}


/* Commit the transaction of a table database object. */
bool tctdbtrancommit(TCTDB *tdb){
  assert(tdb);
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(!tdb->open || !tdb->wmode || !tdb->tran){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  tdb->tran = false;
  bool err = false;
  if(!tctdbtrancommitimpl(tdb)) err = true;
  TDBUNLOCKMETHOD(tdb);
  return !err;
}


/* Abort the transaction of a table database object. */
bool tctdbtranabort(TCTDB *tdb){
  assert(tdb);
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(!tdb->open || !tdb->wmode || !tdb->tran){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  tdb->tran = false;
  bool err = false;
  if(!tctdbtranabortimpl(tdb)) err = true;
  TDBUNLOCKMETHOD(tdb);
  return !err;
}


/* Get the file path of a table database object. */
const char *tctdbpath(TCTDB *tdb){
  assert(tdb);
  if(!TDBLOCKMETHOD(tdb, false)) return NULL;
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return NULL;
  }
  const char *rv = tchdbpath(tdb->hdb);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Get the number of records of a table database object. */
uint64_t tctdbrnum(TCTDB *tdb){
  assert(tdb);
  if(!TDBLOCKMETHOD(tdb, false)) return 0;
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return 0;
  }
  uint64_t rv = tchdbrnum(tdb->hdb);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Get the size of the database file of a table database object. */
uint64_t tctdbfsiz(TCTDB *tdb){
  assert(tdb);
  if(!TDBLOCKMETHOD(tdb, false)) return 0;
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return 0;
  }
  uint64_t rv = tchdbfsiz(tdb->hdb);
  TDBIDX *idxs = tdb->idxs;
  int inum = tdb->inum;
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITLEXICAL:
    case TDBITDECIMAL:
    case TDBITTOKEN:
      rv += tcbdbfsiz(idx->db);
      break;
    }
  }
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Set a column index to a table database object. */
bool tctdbsetindex(TCTDB *tdb, const char *name, int type){
  assert(tdb && name);
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(!tdb->open || !tdb->wmode || tdb->tran){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  bool rv = tctdbsetindeximpl(tdb, name, type);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Generate a unique ID number of a table database object. */
int64_t tctdbgenuid(TCTDB *tdb){
  assert(tdb);
  if(!TDBLOCKMETHOD(tdb, true)) return -1;
  if(!tdb->open || !tdb->wmode){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return -1;
  }
  int64_t rv = tctdbgenuidimpl(tdb, 1);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Create a query object. */
TDBQRY *tctdbqrynew(TCTDB *tdb){
  assert(tdb);
  TDBQRY *qry;
  TCMALLOC(qry, sizeof(*qry));
  qry->tdb = tdb;
  TCMALLOC(qry->conds, sizeof(qry->conds[0]) * 2);
  qry->cnum = 0;
  qry->oname = NULL;
  qry->otype = TDBQOSTRASC;
  qry->max = INT_MAX;
  qry->skip = 0;
  qry->hint = tcxstrnew3(TDBHINTUSIZ);
  qry->count = 0;
  return qry;
}


/* Delete a query object. */
void tctdbqrydel(TDBQRY *qry){
  assert(qry);
  tcxstrdel(qry->hint);
  TCFREE(qry->oname);
  TDBCOND *conds = qry->conds;
  int cnum = qry->cnum;
  for(int i = 0; i < cnum; i++){
    TDBCOND *cond = conds + i;
    TCFREE(cond->expr);
    TCFREE(cond->name);
  }
  TCFREE(conds);
  TCFREE(qry);
}


/* Add a narrowing condition to a query object. */
void tctdbqryaddcond(TDBQRY *qry, const char *name, int op, const char *expr){
  assert(qry && name && expr);
  int cnum = qry->cnum;
  TCREALLOC(qry->conds, qry->conds, sizeof(qry->conds[0]) * (cnum + 1));
  TDBCOND *cond = qry->conds + cnum;
  int nsiz = strlen(name);
  int esiz = strlen(expr);
  TCMEMDUP(cond->name, name, nsiz);
  cond->nsiz = nsiz;
  bool sign = true;
  if(op & TDBQCNEGATE){
    op &= ~TDBQCNEGATE;
    sign = false;
  }
  bool noidx = false;
  if(op & TDBQCNOIDX){
    op &= ~TDBQCNOIDX;
    noidx = true;
  }
  cond->op = op;
  cond->sign = sign;
  cond->noidx = noidx;
  TCMEMDUP(cond->expr, expr, esiz);
  cond->esiz = esiz;
  qry->cnum++;
}


/* Set the order of a query object. */
void tctdbqrysetorder(TDBQRY *qry, const char *name, int type){
  assert(qry && name);
  if(qry->oname) TCFREE(qry->oname);
  qry->oname = tcstrdup(name);
  qry->otype = type;
}


/* Set the limit number of records of the result of a query object. */
void tctdbqrysetlimit(TDBQRY *qry, int max, int skip){
  assert(qry);
  qry->max = (max >= 0) ? max : INT_MAX;
  qry->skip = (skip > 0) ? skip : 0;
}


/* Execute the search of a query object. */
TCLIST *tctdbqrysearch(TDBQRY *qry){
  assert(qry);
  TCTDB *tdb = qry->tdb;
  if(!TDBLOCKMETHOD(tdb, false)) return tclistnew();
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return tclistnew();
  }
  TCLIST *rv = tctdbqrysearchimpl(qry);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Remove each record corresponding to a query object. */
bool tctdbqrysearchout(TDBQRY *qry){
  assert(qry);
  return tctdbqryproc(qry, tctdbqryprocoutcb, NULL);
}


/* Process each record corresponding to a query object. */
bool tctdbqryproc(TDBQRY *qry, TDBQRYPROC proc, void *op){
  assert(qry && proc);
  TCTDB *tdb = qry->tdb;
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(!tdb->open || !tdb->wmode){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  bool err = false;
  int64_t getnum = 0;
  int64_t putnum = 0;
  int64_t outnum = 0;
  TCLIST *res = tctdbqrysearchimpl(qry);
  int rnum = TCLISTNUM(res);
  for(int i = 0; i < rnum; i++){
    const char *pkbuf;
    int pksiz;
    TCLISTVAL(pkbuf, res, i, pksiz);
    TCMAP *cols = tctdbgetimpl(tdb, pkbuf, pksiz);
    if(!cols){
      err = true;
      continue;
    }
    getnum++;
    int flags = proc(pkbuf, pksiz, cols, op);
    if(flags & TDBQPPUT){
      if(tctdbputimpl(tdb, pkbuf, pksiz, cols, TDBPDOVER)){
        putnum++;
      } else {
        err = true;
      }
    } else if(flags & TDBQPOUT){
      if(tctdboutimpl(tdb, pkbuf, pksiz)){
        outnum++;
      } else {
        err = true;
      }
    }
    tcmapdel(cols);
    if(flags & TDBQPSTOP) break;
  }
  tclistdel(res);
  tcxstrprintf(qry->hint, "post treatment: get=%lld, put=%lld, out=%lld\n",
               (long long)getnum, (long long)putnum, (long long)outnum);
  TDBUNLOCKMETHOD(tdb);
  return !err;
}


/* Get the hint of a query object. */
const char *tctdbqryhint(TDBQRY *qry){
  assert(qry);
  return tcxstrptr(qry->hint);
}



/*************************************************************************************************
 * features for experts
 *************************************************************************************************/


/* Set the error code of a table database object. */
void tctdbsetecode(TCTDB *tdb, int ecode, const char *filename, int line, const char *func){
  assert(tdb && filename && line >= 1 && func);
  tchdbsetecode(tdb->hdb, ecode, filename, line, func);
}


/* Set the file descriptor for debugging output. */
void tctdbsetdbgfd(TCTDB *tdb, int fd){
  assert(tdb && fd >= 0);
  tchdbsetdbgfd(tdb->hdb, fd);
}


/* Get the file descriptor for debugging output. */
int tctdbdbgfd(TCTDB *tdb){
  assert(tdb);
  return tchdbdbgfd(tdb->hdb);
}


/* Check whether mutual exclusion control is set to a table database object. */
bool tctdbhasmutex(TCTDB *tdb){
  assert(tdb);
  return tdb->mmtx != NULL;
}


/* Synchronize updating contents on memory of a table database object. */
bool tctdbmemsync(TCTDB *tdb, bool phys){
  assert(tdb);
  if(!tdb->open || !tdb->wmode){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return false;
  }
  bool err = false;
  if(!tchdbmemsync(tdb->hdb, phys)) err = true;
  TDBIDX *idxs = tdb->idxs;
  int inum = tdb->inum;
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITTOKEN:
      if(!tctdbidxsynctoken(tdb, idx)) err = true;
      break;
    }
  }
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITLEXICAL:
    case TDBITDECIMAL:
    case TDBITTOKEN:
      if(!tcbdbmemsync(idx->db, phys)){
        tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
        err = true;
      }
      break;
    }
  }
  return !err;
}


/* Get the number of elements of the bucket array of a table database object. */
uint64_t tctdbbnum(TCTDB *tdb){
  assert(tdb);
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return 0;
  }
  return tchdbbnum(tdb->hdb);
}


/* Get the record alignment of a table database object. */
uint32_t tctdbalign(TCTDB *tdb){
  assert(tdb);
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return 0;
  }
  return tchdbalign(tdb->hdb);
}


/* Get the maximum number of the free block pool of a table database object. */
uint32_t tctdbfbpmax(TCTDB *tdb){
  assert(tdb);
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return 0;
  }
  return tchdbfbpmax(tdb->hdb);
}


/* Get the inode number of the database file of a table database object. */
uint64_t tctdbinode(TCTDB *tdb){
  assert(tdb);
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return 0;
  }
  return tchdbinode(tdb->hdb);
}


/* Get the modification time of the database file of a table database object. */
time_t tctdbmtime(TCTDB *tdb){
  assert(tdb);
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return 0;
  }
  return tchdbmtime(tdb->hdb);
}


/* Get the additional flags of a table database object. */
uint8_t tctdbflags(TCTDB *tdb){
  assert(tdb);
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return 0;
  }
  return tchdbflags(tdb->hdb);
}


/* Get the options of a table database object. */
uint8_t tctdbopts(TCTDB *tdb){
  assert(tdb);
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return 0;
  }
  return tdb->opts;
}


/* Get the pointer to the opaque field of a table database object. */
char *tctdbopaque(TCTDB *tdb){
  assert(tdb);
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return NULL;
  }
  return tchdbopaque(tdb->hdb) + TDBOPAQUESIZ;
}


/* Get the number of used elements of the bucket array of a table database object. */
uint64_t tctdbbnumused(TCTDB *tdb){
  assert(tdb);
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return 0;
  }
  return tchdbbnumused(tdb->hdb);
}


/* Get the number of column indices of a table database object. */
int tctdbinum(TCTDB *tdb){
  assert(tdb);
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    return 0;
  }
  return tdb->inum;
}


/* Get the seed of unique ID unumbers of a table database object. */
int64_t tctdbuidseed(TCTDB *tdb){
  assert(tdb);
  if(!TDBLOCKMETHOD(tdb, false)) return -1;
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return -1;
  }
  int64_t rv = tctdbgenuidimpl(tdb, 0);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Set the seed of unique ID unumbers of a table database object. */
bool tctdbsetuidseed(TCTDB *tdb, int64_t seed){
  assert(tdb && seed >= 0);
  if(!TDBLOCKMETHOD(tdb, true)) return -1;
  if(!tdb->open || !tdb->wmode){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  tctdbgenuidimpl(tdb, -seed - 1);
  TDBUNLOCKMETHOD(tdb);
  return true;
}


/* Set the custom codec functions of a table database object. */
bool tctdbsetcodecfunc(TCTDB *tdb, TCCODEC enc, void *encop, TCCODEC dec, void *decop){
  assert(tdb && enc && dec);
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  bool rv = tchdbsetcodecfunc(tdb->hdb, enc, encop, dec, decop);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Get the unit step number of auto defragmentation of a table database object. */
uint32_t tctdbdfunit(TCTDB *tdb){
  assert(tdb);
  return tchdbdfunit(tdb->hdb);
}


/* Perform dynamic defragmentation of a table database object. */
bool tctdbdefrag(TCTDB *tdb, int64_t step){
  assert(tdb);
  if(!TDBLOCKMETHOD(tdb, false)) return false;
  if(!tdb->open || !tdb->wmode){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  bool rv = tctdbdefragimpl(tdb, step);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Store a record into a table database object with a duplication handler. */
bool tctdbputproc(TCTDB *tdb, const void *pkbuf, int pksiz, const void *cbuf, int csiz,
                  TCPDPROC proc, void *op){
  assert(tdb && pkbuf && pksiz >= 0 && proc);
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(!tdb->open || !tdb->wmode){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  bool err = false;
  TCMAP *cols = tctdbgetimpl(tdb, pkbuf, pksiz);
  if(cols){
    int zsiz;
    char *zbuf = tcstrjoin4(cols, &zsiz);
    int ncsiz;
    void *ncbuf = proc(zbuf, zsiz, &ncsiz, op);
    if(ncbuf == (void *)-1){
      if(!tctdboutimpl(tdb, pkbuf, pksiz)) err = true;
    } else if(ncbuf){
      TCMAP *ncols = tcstrsplit4(ncbuf, ncsiz);
      if(!tctdbputimpl(tdb, pkbuf, pksiz, ncols, TDBPDOVER)) err = true;
      tcmapdel(ncols);
      TCFREE(ncbuf);
    } else {
      tctdbsetecode(tdb, TCEKEEP, __FILE__, __LINE__, __func__);
      err = true;
    }
    TCFREE(zbuf);
    tcmapdel(cols);
  } else {
    if(cbuf){
      cols = tcstrsplit4(cbuf, csiz);
      if(!tctdbputimpl(tdb, pkbuf, pksiz, cols, TDBPDOVER)) err = true;
      tcmapdel(cols);
    } else {
      tctdbsetecode(tdb, TCENOREC, __FILE__, __LINE__, __func__);
      err = true;
    }
  }
  TDBUNLOCKMETHOD(tdb);
  return !err;
}


/* Move the iterator to the record corresponding a key of a table database object. */
bool tctdbiterinit2(TCTDB *tdb, const void *pkbuf, int pksiz){
  assert(tdb && pkbuf && pksiz >= 0);
  if(!TDBLOCKMETHOD(tdb, true)) return false;
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  bool rv = tchdbiterinit2(tdb->hdb, pkbuf, pksiz);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Move the iterator to the record corresponding a key string of a table database object. */
bool tctdbiterinit3(TCTDB *tdb, const char *kstr){
  assert(tdb && kstr);
  return tctdbiterinit2(tdb, kstr, strlen(kstr));
}


/* Process each record atomically of a table database object. */
bool tctdbforeach(TCTDB *tdb, TCITER iter, void *op){
  assert(tdb && iter);
  if(!TDBLOCKMETHOD(tdb, false)) return false;
  if(!tdb->open){
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    TDBUNLOCKMETHOD(tdb);
    return false;
  }
  TDBTHREADYIELD(tdb);
  bool rv = tctdbforeachimpl(tdb, iter, op);
  TDBUNLOCKMETHOD(tdb);
  return rv;
}


/* Process each record corresponding to a query object with non-atomic fashion. */
bool tctdbqryproc2(TDBQRY *qry, TDBQRYPROC proc, void *op){
  assert(qry && proc);
  TCTDB *tdb = qry->tdb;
  TDBCOND *conds = qry->conds;
  int cnum = qry->cnum;
  bool err = false;
  int64_t getnum = 0;
  int64_t putnum = 0;
  int64_t outnum = 0;
  TCLIST *res = tctdbqrysearch(qry);
  int rnum = TCLISTNUM(res);
  for(int i = 0; i < rnum; i++){
    if(!TDBLOCKMETHOD(tdb, true)){
      err = true;
      break;
    }
    if(!tdb->open || !tdb->wmode){
      tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
      TDBUNLOCKMETHOD(tdb);
      err = true;
      break;
    }
    int pksiz;
    const char *pkbuf;
    TCLISTVAL(pkbuf, res, i, pksiz);
    TCMAP *cols = tctdbgetimpl(tdb, pkbuf, pksiz);
    if(cols){
      getnum++;
      bool ok = true;
      for(int j = 0; j < cnum; j++){
        TDBCOND *cond = conds + j;
        if(cond->nsiz < 1){
          if(tctdbqrycondmatch(cond->op, cond->expr, cond->esiz, pkbuf, pksiz) != cond->sign){
            ok = false;
            break;
          }
        } else {
          int vsiz;
          const char *vbuf = tcmapget(cols, cond->name, cond->nsiz, &vsiz);
          if(vbuf){
            if(tctdbqrycondmatch(cond->op, cond->expr, cond->esiz, vbuf, vsiz) != cond->sign){
              ok = false;
              break;
            }
          } else {
            if(cond->sign){
              ok = false;
              break;
            }
          }
        }
      }
      if(ok){
        int flags = proc(pkbuf, pksiz, cols, op);
        if(flags & TDBQPPUT){
          if(tctdbputimpl(tdb, pkbuf, pksiz, cols, TDBPDOVER)){
            putnum++;
          } else {
            err = true;
          }
        } else if(flags & TDBQPOUT){
          if(tctdboutimpl(tdb, pkbuf, pksiz)){
            outnum++;
          } else {
            err = true;
          }
        }
        if(flags & TDBQPSTOP) i = rnum;
      }
      tcmapdel(cols);
    }
    TDBUNLOCKMETHOD(tdb);
  }
  tclistdel(res);
  tcxstrprintf(qry->hint, "post treatment: get=%lld, put=%lld, out=%lld\n",
               (long long)getnum, (long long)putnum, (long long)outnum);
  return !err;
}


/* Remove each record corresponding to a query object with non-atomic fashion. */
bool tctdbqrysearchout2(TDBQRY *qry){
  assert(qry);
  return tctdbqryproc2(qry, tctdbqryprocoutcb, NULL);
}


/* Convert a string into the index type number. */
int tctdbstrtoindextype(const char *str){
  assert(str);
  int type = -1;
  int flags = 0;
  if(*str == '+'){
    flags |= TDBITKEEP;
    str++;
  }
  if(!tcstricmp(str, "LEX") || !tcstricmp(str, "LEXICAL") || !tcstricmp(str, "STR")){
    type = TDBITLEXICAL;
  } else if(!tcstricmp(str, "DEC") || !tcstricmp(str, "DECIMAL") || !tcstricmp(str, "NUM")){
    type = TDBITDECIMAL;
  } else if(!tcstricmp(str, "TOK") || !tcstricmp(str, "TOKEN")){
    type = TDBITTOKEN;
  } else if(!tcstricmp(str, "OPT") || !tcstricmp(str, "OPTIMIZE")){
    type = TDBITOPT;
  } else if(!tcstricmp(str, "VOID") || !tcstricmp(str, "NULL")){
    type = TDBITVOID;
  } else if(tcstrisnum(str)){
    type = tcatoi(str);
  }
  return type | flags;
}


/* Get the count of corresponding records of a query object. */
int tctdbqrycount(TDBQRY *qry){
  assert(qry);
  return qry->count;
}


/* Convert a string into the query operation number. */
int tctdbqrystrtocondop(const char *str){
  assert(str);
  int op = -1;
  int flags = 0;
  if(*str == '~' || *str == '!'){
    flags |= TDBQCNEGATE;
    str++;
  }
  if(*str == '+'){
    flags |= TDBQCNOIDX;
    str++;
  }
  if(!tcstricmp(str, "STREQ")){
    op = TDBQCSTREQ;
  } else if(!tcstricmp(str, "STRINC")){
    op = TDBQCSTRINC;
  } else if(!tcstricmp(str, "STRBW")){
    op = TDBQCSTRBW;
  } else if(!tcstricmp(str, "STREW")){
    op = TDBQCSTREW;
  } else if(!tcstricmp(str, "STRAND")){
    op = TDBQCSTRAND;
  } else if(!tcstricmp(str, "STROR")){
    op = TDBQCSTROR;
  } else if(!tcstricmp(str, "STROREQ")){
    op = TDBQCSTROREQ;
  } else if(!tcstricmp(str, "STRRX")){
    op = TDBQCSTRRX;
  } else if(!tcstricmp(str, "NUMEQ")){
    op = TDBQCNUMEQ;
  } else if(!tcstricmp(str, "NUMGT")){
    op = TDBQCNUMGT;
  } else if(!tcstricmp(str, "NUMGE")){
    op = TDBQCNUMGE;
  } else if(!tcstricmp(str, "NUMLT")){
    op = TDBQCNUMLT;
  } else if(!tcstricmp(str, "NUMLE")){
    op = TDBQCNUMLE;
  } else if(!tcstricmp(str, "NUMBT")){
    op = TDBQCNUMBT;
  } else if(!tcstricmp(str, "NUMOREQ")){
    op = TDBQCNUMOREQ;
  } else if(tcstrisnum(str)){
    op = tcatoi(str);
  }
  return op | flags;
}


/* Convert a string into the query order type number. */
int tctdbqrystrtoordertype(const char *str){
  assert(str);
  int type = -1;
  if(!tcstricmp(str, "STRASC") || !tcstricmp(str, "STR")){
    type = TDBQOSTRASC;
  } else if(!tcstricmp(str, "STRDESC")){
    type = TDBQOSTRDESC;
  } else if(!tcstricmp(str, "NUMASC") || !tcstricmp(str, "NUM")){
    type = TDBQONUMASC;
  } else if(!tcstricmp(str, "NUMDESC")){
    type = TDBQONUMDESC;
  } else if(tcstrisnum(str)){
    type = tcatoi(str);
  }
  return type;
}



/*************************************************************************************************
 * private features
 *************************************************************************************************/


/* Clear all members.
   `tdb' specifies the table database object. */
static void tctdbclear(TCTDB *tdb){
  assert(tdb);
  tdb->mmtx = NULL;
  tdb->hdb = NULL;
  tdb->open = false;
  tdb->wmode = false;
  tdb->opts = 0;
  tdb->lcnum = TDBDEFLCNUM;
  tdb->ncnum = TDBDEFNCNUM;
  tdb->idxs = NULL;
  tdb->inum = 0;
  tdb->tran = false;
}


/* Open a database file and connect a table database object.
   `tdb' specifies the table database object.
   `path' specifies the path of the internal database file.
   `omode' specifies the connection mode.
   If successful, the return value is true, else, it is false. */
static bool tctdbopenimpl(TCTDB *tdb, const char *path, int omode){
  assert(tdb && path);
  int dbgfd = tchdbdbgfd(tdb->hdb);
  TCCODEC enc, dec;
  void *encop, *decop;
  tchdbcodecfunc(tdb->hdb, &enc, &encop, &dec, &decop);
  int homode = HDBOREADER;
  int bomode = BDBOREADER;
  if(omode & TDBOWRITER){
    homode = HDBOWRITER;
    bomode = BDBOWRITER;
    if(omode & TDBOCREAT){
      homode |= HDBOCREAT;
      bomode |= BDBOCREAT;
    }
    if(omode & TDBOTRUNC){
      homode |= HDBOTRUNC;
      bomode |= BDBOTRUNC;
    }
    tdb->wmode = true;
  } else {
    tdb->wmode = false;
  }
  if(omode & TDBONOLCK){
    homode |= HDBONOLCK;
    bomode |= BDBONOLCK;
  }
  if(omode & TDBOLCKNB){
    homode |= HDBOLCKNB;
    bomode |= BDBOLCKNB;
  }
  if(omode & TDBOTSYNC){
    homode |= HDBOTSYNC;
    bomode |= BDBOTSYNC;
  }
  tchdbsettype(tdb->hdb, TCDBTTABLE);
  if(!tchdbopen(tdb->hdb, path, homode)) return false;
  char *tpath = tcsprintf("%s%c%s%c*", path, MYEXTCHR, TDBIDXSUFFIX, MYEXTCHR);
  if((omode & TDBOWRITER) && (omode & TDBOTRUNC)){
    TCLIST *paths = tcglobpat(tpath);
    int pnum = TCLISTNUM(paths);
    for(int i = 0; i < pnum; i++){
      unlink(TCLISTVALPTR(paths, i));
    }
    tclistdel(paths);
  }
  TCLIST *paths = tcglobpat(tpath);
  int pnum = TCLISTNUM(paths);
  TCMALLOC(tdb->idxs, sizeof(tdb->idxs[0]) * pnum + 1);
  TDBIDX *idxs = tdb->idxs;
  int inum = 0;
  for(int i = 0; i < pnum; i++){
    const char *ipath = TCLISTVALPTR(paths, i);
    if(!tcstrfwm(ipath, path)) continue;
    const char *rp = ipath + strlen(path);
    if(*rp != MYEXTCHR) continue;
    rp++;
    if(!tcstrfwm(rp, TDBIDXSUFFIX)) continue;
    rp += strlen(TDBIDXSUFFIX);
    if(*rp != MYEXTCHR) continue;
    rp++;
    char *stem = tcstrdup(rp);
    char *ep = strrchr(stem, MYEXTCHR);
    if(!ep) continue;
    *(ep++) = '\0';
    int nsiz;
    char *name = tcurldecode(stem, &nsiz);
    if(!strcmp(ep, "lex") || !strcmp(ep, "dec") || !strcmp(ep, "tok")){
      TCBDB *bdb = tcbdbnew();
      if(dbgfd >= 0) tcbdbsetdbgfd(bdb, dbgfd);
      if(tdb->mmtx) tcbdbsetmutex(bdb);
      if(enc && dec) tcbdbsetcodecfunc(bdb, enc, encop, dec, decop);
      tcbdbsetcache(bdb, tdb->lcnum, tdb->ncnum);
      tcbdbsetdfunit(bdb, tchdbdfunit(tdb->hdb));
      tcbdbsetlsmax(bdb, TDBIDXLSMAX);
      if(tcbdbopen(bdb, ipath, bomode)){
        idxs[inum].name = tcstrdup(name);
        idxs[inum].type = TDBITLEXICAL;
        if(!strcmp(ep, "dec")){
          idxs[inum].type = TDBITDECIMAL;
        } else if(!strcmp(ep, "tok")){
          idxs[inum].type = TDBITTOKEN;
        }
        idxs[inum].db = bdb;
        idxs[inum].cc = NULL;
        if(idxs[inum].type == TDBITTOKEN) idxs[inum].cc = tcmapnew2(TDBIDXCCBNUM);
        inum++;
      } else {
        tcbdbdel(bdb);
      }
    }
    TCFREE(name);
    TCFREE(stem);
  }
  tclistdel(paths);
  TCFREE(tpath);
  tdb->inum = inum;
  tdb->open = true;
  uint8_t hopts = tchdbopts(tdb->hdb);
  uint8_t opts = 0;
  if(hopts & HDBTLARGE) opts |= TDBTLARGE;
  if(hopts & HDBTDEFLATE) opts |= TDBTDEFLATE;
  if(hopts & HDBTBZIP) opts |= TDBTBZIP;
  if(hopts & HDBTTCBS) opts |= TDBTTCBS;
  if(hopts & HDBTEXCODEC) opts |= TDBTEXCODEC;
  tdb->opts = opts;
  tdb->tran = false;
  return true;
}


/* Close a table database object.
   `tdb' specifies the table database object.
   If successful, the return value is true, else, it is false. */
static bool tctdbcloseimpl(TCTDB *tdb){
  assert(tdb);
  bool err = false;
  if(tdb->tran && !tctdbtranabortimpl(tdb)) err = true;
  TDBIDX *idxs = tdb->idxs;
  int inum = tdb->inum;
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITTOKEN:
      if(!tctdbidxsynctoken(tdb, idx)) err = true;
      tcmapdel(idx->cc);
      break;
    }
  }
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITLEXICAL:
    case TDBITDECIMAL:
    case TDBITTOKEN:
      if(!tcbdbclose(idx->db)){
        tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
        err = true;
      }
      tcbdbdel(idx->db);
      break;
    }
    TCFREE(idx->name);
  }
  TCFREE(idxs);
  if(!tchdbclose(tdb->hdb)) err = true;
  tdb->open = false;
  return !err;
}


/* Store a record into a table database object.
   `tdb' specifies the table database object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   `cols' specifies a map object containing columns.
   `dmode' specifies behavior when the key overlaps.
   If successful, the return value is true, else, it is false. */
static bool tctdbputimpl(TCTDB *tdb, const void *pkbuf, int pksiz, TCMAP *cols, int dmode){
  assert(tdb && pkbuf && pksiz >= 0 && cols);
  bool err = false;
  int osiz;
  char *obuf = tchdbget(tdb->hdb, pkbuf, pksiz, &osiz);
  if(obuf){
    if(dmode == TDBPDKEEP){
      tctdbsetecode(tdb, TCEKEEP, __FILE__, __LINE__, __func__);
      TCFREE(obuf);
      return false;
    }
    TCMAP *ocols = tcmapload(obuf, osiz);
    if(dmode == TDBPDCAT){
      TCMAP *ncols = tcmapnew2(tcmaprnum(cols) + 1);
      tcmapiterinit(cols);
      const char *kbuf;
      int ksiz;
      while((kbuf = tcmapiternext(cols, &ksiz)) != NULL){
        int vsiz;
        const char *vbuf = tcmapiterval(kbuf, &vsiz);
        if(tcmapputkeep(ocols, kbuf, ksiz, vbuf, vsiz)) tcmapput(ncols, kbuf, ksiz, vbuf, vsiz);
      }
      if(!tctdbidxput(tdb, pkbuf, pksiz, ncols)) err = true;
      tcmapdel(ncols);
      int csiz;
      char *cbuf = tcmapdump(ocols, &csiz);
      if(!tchdbput(tdb->hdb, pkbuf, pksiz, cbuf, csiz)) err = true;
      TCFREE(cbuf);
    } else {
      TCMAP *ncols = tcmapnew2(tcmaprnum(cols) + 1);
      tcmapiterinit(cols);
      const char *kbuf;
      int ksiz;
      while((kbuf = tcmapiternext(cols, &ksiz)) != NULL){
        int vsiz;
        const char *vbuf = tcmapiterval(kbuf, &vsiz);
        int osiz;
        const char *obuf = tcmapget(ocols, kbuf, ksiz, &osiz);
        if(obuf && osiz == vsiz && !memcmp(obuf, vbuf, osiz)){
          tcmapout(ocols, kbuf, ksiz);
        } else {
          tcmapput(ncols, kbuf, ksiz, vbuf, vsiz);
        }
      }
      if(!tctdbidxout(tdb, pkbuf, pksiz, ocols)) err = true;
      if(!tctdbidxput(tdb, pkbuf, pksiz, ncols)) err = true;
      tcmapdel(ncols);
      int csiz;
      char *cbuf = tcmapdump(cols, &csiz);
      if(!tchdbput(tdb->hdb, pkbuf, pksiz, cbuf, csiz)) err = true;
      TCFREE(cbuf);
    }
    tcmapdel(ocols);
    TCFREE(obuf);
  } else {
    if(!tctdbidxput(tdb, pkbuf, pksiz, cols)) err = true;
    int csiz;
    char *cbuf = tcmapdump(cols, &csiz);
    if(!tchdbput(tdb->hdb, pkbuf, pksiz, cbuf, csiz)) err = true;
    TCFREE(cbuf);
  }
  return !err;
}


/* Remove a record of a table database object.
   `tdb' specifies the table database object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   If successful, the return value is true, else, it is false. */
static bool tctdboutimpl(TCTDB *tdb, const char *pkbuf, int pksiz){
  assert(tdb && pkbuf && pksiz >= 0);
  int csiz;
  char *cbuf = tchdbget(tdb->hdb, pkbuf, pksiz, &csiz);
  if(!cbuf) return false;
  bool err = false;
  TCMAP *cols = tcmapload(cbuf, csiz);
  if(!tctdbidxout(tdb, pkbuf, pksiz, cols)) err = true;
  if(!tchdbout(tdb->hdb, pkbuf, pksiz)) err = true;
  tcmapdel(cols);
  TCFREE(cbuf);
  return !err;
}


/* Retrieve a record in a table database object.
   `tdb' specifies the table database object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   If successful, the return value is a map object of the columns of the corresponding record. */
static TCMAP *tctdbgetimpl(TCTDB *tdb, const void *pkbuf, int pksiz){
  assert(tdb && pkbuf && pksiz >= 0);
  int csiz;
  char *cbuf = tchdbget(tdb->hdb, pkbuf, pksiz, &csiz);
  if(!cbuf) return false;
  TCMAP *cols = tcmapload(cbuf, csiz);
  TCFREE(cbuf);
  return cols;
}


/* Add a real number to a column of a record in a table database object.
   `tdb' specifies the table database object.
   `kbuf' specifies the pointer to the region of the primary key.
   `ksiz' specifies the size of the region of the primary key.
   `num' specifies the additional value.
   If successful, the return value is the summation value, else, it is Not-a-Number. */
static double tctdbaddnumber(TCTDB *tdb, const void *pkbuf, int pksiz, double num){
  assert(tdb && pkbuf && pksiz >= 0);
  int csiz;
  char *cbuf = tchdbget(tdb->hdb, pkbuf, pksiz, &csiz);
  TCMAP *cols = cbuf ? tcmapload(cbuf, csiz) : tcmapnew2(1);
  if(cbuf){
    const char *vbuf = tcmapget2(cols, TDBNUMCNTCOL);
    if(vbuf) num += tcatof(vbuf);
    TCFREE(cbuf);
  }
  char numbuf[TDBCOLBUFSIZ];
  int len = snprintf(numbuf, TDBCOLBUFSIZ - 1, "%f", num);
  if(len > TDBCOLBUFSIZ - 1){
    tctdbsetecode(tdb, TCEMISC, __FILE__, __LINE__, __func__);
    num = nan("");
  } else {
    while(--len > 0){
      if(numbuf[len] != '0') break;
      numbuf[len] = '\0';
    }
    if(numbuf[len] == '.') numbuf[len] = '\0';
    tcmapput2(cols, TDBNUMCNTCOL, numbuf);
    if(!tctdbputimpl(tdb, pkbuf, pksiz, cols, TDBPDOVER)) num = nan("");
  }
  tcmapdel(cols);

  return num;
}


/* Optimize the file of a table database object.
   `tdb' specifies the table database object.
   `bnum' specifies the number of elements of the bucket array.
   `apow' specifies the size of record alignment by power of 2.
   `fpow' specifies the maximum number of elements of the free block pool by power of 2.
   `opts' specifies options by bitwise-or.
   If successful, the return value is true, else, it is false. */
static bool tctdboptimizeimpl(TCTDB *tdb, int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts){
  assert(tdb);
  bool err = false;
  TCHDB *hdb = tdb->hdb;
  TDBIDX *idxs = tdb->idxs;
  int inum = tdb->inum;
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITTOKEN:
      tcmapclear(idx->cc);
      break;
    }
  }
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITLEXICAL:
    case TDBITDECIMAL:
    case TDBITTOKEN:
      if(!tcbdbvanish(idx->db)){
        tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
        err = true;
      }
      break;
    }
  }
  const char *path = tchdbpath(tdb->hdb);
  char *tpath = tcsprintf("%s%ctmp%c%llu", path, MYEXTCHR, MYEXTCHR, tchdbinode(tdb->hdb));
  TCHDB *thdb = tchdbnew();
  tchdbsettype(thdb, TCDBTTABLE);
  int dbgfd = tchdbdbgfd(tdb->hdb);
  if(dbgfd >= 0) tchdbsetdbgfd(thdb, dbgfd);
  TCCODEC enc, dec;
  void *encop, *decop;
  tchdbcodecfunc(hdb, &enc, &encop, &dec, &decop);
  if(enc && dec) tchdbsetcodecfunc(thdb, enc, encop, dec, decop);
  if(bnum < 1) bnum = tchdbrnum(hdb) * 2 + 1;
  if(apow < 0) apow = tclog2l(tchdbalign(hdb));
  if(fpow < 0) fpow = tclog2l(tchdbfbpmax(hdb));
  if(opts == UINT8_MAX) opts = tdb->opts;
  uint8_t hopts = 0;
  if(opts & TDBTLARGE) hopts |= HDBTLARGE;
  if(opts & TDBTDEFLATE) hopts |= HDBTDEFLATE;
  if(opts & TDBTBZIP) hopts |= HDBTBZIP;
  if(opts & TDBTTCBS) hopts |= HDBTTCBS;
  if(opts & TDBTEXCODEC) hopts |= HDBTEXCODEC;
  tchdbtune(thdb, bnum, apow, fpow, hopts);
  if(tchdbopen(thdb, tpath, HDBOWRITER | HDBOCREAT | HDBOTRUNC)){
    memcpy(tchdbopaque(thdb), tchdbopaque(hdb), TDBOPAQUESIZ + TDBLEFTOPQSIZ);
    if(!tchdbiterinit(hdb)) err = true;
    TCXSTR *kxstr = tcxstrnew();
    TCXSTR *vxstr = tcxstrnew();
    while(tchdbiternext3(hdb, kxstr, vxstr)){
      TCMAP *cols = tcmapload(TCXSTRPTR(vxstr), TCXSTRSIZE(vxstr));
      if(!tctdbidxput(tdb, TCXSTRPTR(kxstr), TCXSTRSIZE(kxstr), cols)) err = true;
      tcmapdel(cols);
      if(!tchdbput(thdb, TCXSTRPTR(kxstr), TCXSTRSIZE(kxstr),
                   TCXSTRPTR(vxstr), TCXSTRSIZE(vxstr))){
        tctdbsetecode(tdb, tchdbecode(thdb), __FILE__, __LINE__, __func__);
        err = true;
      }
    }
    tcxstrdel(vxstr);
    tcxstrdel(kxstr);
    if(!tchdbclose(thdb)){
      tctdbsetecode(tdb, tchdbecode(thdb), __FILE__, __LINE__, __func__);
      err = true;
    }
    if(!err){
      if(unlink(path) == -1){
        tctdbsetecode(tdb, TCEUNLINK, __FILE__, __LINE__, __func__);
        err = true;
      }
      if(rename(tpath, path) == -1){
        tctdbsetecode(tdb, TCERENAME, __FILE__, __LINE__, __func__);
        err = true;
      }
      char *npath = tcstrdup(path);
      int omode = (tchdbomode(hdb) & ~HDBOCREAT) & ~HDBOTRUNC;
      if(!tchdbclose(hdb)) err = true;
      if(!tchdbopen(hdb, npath, omode)) err = true;
      TCFREE(npath);
    }
  } else {
    tctdbsetecode(tdb, tchdbecode(thdb), __FILE__, __LINE__, __func__);
    err = true;
  }
  tchdbdel(thdb);
  TCFREE(tpath);
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITTOKEN:
      if(!tctdbidxsynctoken(tdb, idx)) err = true;
      break;
    }
  }
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITLEXICAL:
    case TDBITDECIMAL:
    case TDBITTOKEN:
      if(!tcbdboptimize(idx->db, -1, -1, -1, -1, -1, UINT8_MAX)){
        tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
        err = true;
      }
      break;
    }
  }
  return !err;
}


/* Remove all records of a table database object.
   `tdb' specifies the table database object.
   If successful, the return value is true, else, it is false. */
static bool tctdbvanishimpl(TCTDB *tdb){
  assert(tdb);
  bool err = false;
  if(!tchdbvanish(tdb->hdb)) err = true;
  TDBIDX *idxs = tdb->idxs;
  int inum = tdb->inum;
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITTOKEN:
      tcmapclear(idx->cc);
      break;
    }
  }
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITLEXICAL:
    case TDBITDECIMAL:
    case TDBITTOKEN:
      if(!tcbdbvanish(idx->db)){
        tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
        err = true;
      }
      break;
    }
  }
  return !err;
}


/* Copy the database file of a table database object.
   `tdb' specifies the table database object.
   `path' specifies the path of the destination file.
   If successful, the return value is true, else, it is false. */
static bool tctdbcopyimpl(TCTDB *tdb, const char *path){
  assert(tdb);
  bool err = false;
  if(!tchdbcopy(tdb->hdb, path)) err = true;
  const char *opath = tchdbpath(tdb->hdb);
  TDBIDX *idxs = tdb->idxs;
  int inum = tdb->inum;
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITTOKEN:
      if(!tctdbidxsynctoken(tdb, idx)) err = true;
      break;
    }
  }
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    const char *ipath;
    switch(idx->type){
    case TDBITLEXICAL:
    case TDBITDECIMAL:
    case TDBITTOKEN:
      if(*path == '@'){
        if(!tcbdbcopy(idx->db, path)){
          tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
          err = true;
        }
      } else {
        ipath = tcbdbpath(idx->db);
        if(tcstrfwm(ipath, opath)){
          char *tpath = tcsprintf("%s%s", path, ipath + strlen(opath));
          if(!tcbdbcopy(idx->db, tpath)){
            tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
            err = true;
          }
          TCFREE(tpath);
        } else {
          tctdbsetecode(tdb, TCEMISC, __FILE__, __LINE__, __func__);
          err = true;
        }
      }
      break;
    }
  }
  return !err;
}


/* Begin the transaction of a table database object.
   `tdb' specifies the table database object.
   If successful, the return value is true, else, it is false. */
static bool tctdbtranbeginimpl(TCTDB *tdb){
  assert(tdb);
  if(!tctdbmemsync(tdb, false)) return false;
  if(!tchdbtranbegin(tdb->hdb)) return false;
  bool err = false;
  TDBIDX *idxs = tdb->idxs;
  int inum = tdb->inum;
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITTOKEN:
      if(!tctdbidxsynctoken(tdb, idx)) err = true;
      break;
    }
  }
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITLEXICAL:
    case TDBITDECIMAL:
    case TDBITTOKEN:
      if(!tcbdbtranbegin(idx->db)){
        tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
        err = true;
      }
      break;
    }
  }
  return !err;
}


/* Commit the transaction of a table database object.
   `tdb' specifies the table database object.
   If successful, the return value is true, else, it is false. */
static bool tctdbtrancommitimpl(TCTDB *tdb){
  assert(tdb);
  bool err = false;
  if(!tctdbmemsync(tdb, false)) err = true;
  if(!tchdbtrancommit(tdb->hdb)) err = true;
  TDBIDX *idxs = tdb->idxs;
  int inum = tdb->inum;
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITTOKEN:
      if(!tctdbidxsynctoken(tdb, idx)) err = true;
      break;
    }
  }
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITLEXICAL:
    case TDBITDECIMAL:
    case TDBITTOKEN:
      if(!tcbdbtrancommit(idx->db)){
        tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
        err = true;
      }
      break;
    }
  }
  return !err;
}


/* Abort the transaction of a table database object.
   `tdb' specifies the table database object.
   If successful, the return value is true, else, it is false. */
static bool tctdbtranabortimpl(TCTDB *tdb){
  assert(tdb);
  bool err = false;
  if(!tchdbtranabort(tdb->hdb)) err = true;
  TDBIDX *idxs = tdb->idxs;
  int inum = tdb->inum;
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITTOKEN:
      tcmapclear(idx->cc);
      break;
    }
  }
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITLEXICAL:
    case TDBITDECIMAL:
    case TDBITTOKEN:
      if(!tcbdbtranabort(idx->db)){
        tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
        err = true;
      }
      break;
    }
  }
  return !err;
}


/* Set a column index to a table database object.
   `tdb' specifies the table database object. connected as a writer.
   `name' specifies the name of a column.
   `type' specifies the index type.
   If successful, the return value is true, else, it is false. */
static bool tctdbsetindeximpl(TCTDB *tdb, const char *name, int type){
  assert(tdb && name);
  bool err = false;
  bool keep = false;
  if(type & TDBITKEEP){
    type &= ~TDBITKEEP;
    keep = true;
  }
  bool done = false;
  TDBIDX *idxs = tdb->idxs;
  int inum = tdb->inum;
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    const char *path;
    if(!strcmp(idx->name, name)){
      if(keep){
        tctdbsetecode(tdb, TCEKEEP, __FILE__, __LINE__, __func__);
        return false;
      }
      if(type == TDBITOPT){
        switch(idx->type){
        case TDBITTOKEN:
          if(!tctdbidxsynctoken(tdb, idx)) err = true;
          break;
        }
        switch(idx->type){
        case TDBITLEXICAL:
        case TDBITDECIMAL:
        case TDBITTOKEN:
          if(!tcbdboptimize(idx->db, -1, -1, -1, -1, -1, UINT8_MAX)){
            tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
            err = true;
          }
          break;
        }
        done = true;
        break;
      }
      switch(idx->type){
      case TDBITTOKEN:
        tcmapdel(idx->cc);
        break;
      }
      switch(idx->type){
      case TDBITLEXICAL:
      case TDBITDECIMAL:
      case TDBITTOKEN:
        path = tcbdbpath(idx->db);
        if(path && unlink(path)){
          tctdbsetecode(tdb, TCEUNLINK, __FILE__, __LINE__, __func__);
          err = true;
        }
        tcbdbdel(idx->db);
        break;
      }
      TCFREE(idx->name);
      tdb->inum--;
      inum = tdb->inum;
      memmove(idxs + i, idxs + i + 1, sizeof(*idxs) * (inum - i));
      done = true;
      break;
    }
  }
  if(type == TDBITOPT || type == TDBITVOID){
    if(!done){
      tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
      err = true;
    }
    return !err;
  }
  TCXSTR *pbuf = tcxstrnew();
  tcxstrprintf(pbuf, "%s%c%s%c%?", tchdbpath(tdb->hdb), MYEXTCHR, TDBIDXSUFFIX, MYEXTCHR, name);
  TCREALLOC(tdb->idxs, tdb->idxs, sizeof(tdb->idxs[0]) * (inum + 1));
  TDBIDX *idx = tdb->idxs + inum;
  int homode = tchdbomode(tdb->hdb);
  int bomode = BDBOWRITER | BDBOCREAT | BDBOTRUNC;
  if(homode & HDBONOLCK) bomode |= BDBONOLCK;
  if(homode & HDBOLCKNB) bomode |= BDBOLCKNB;
  if(homode & HDBOTSYNC) bomode |= BDBOTSYNC;
  int dbgfd = tchdbdbgfd(tdb->hdb);
  TCCODEC enc, dec;
  void *encop, *decop;
  tchdbcodecfunc(tdb->hdb, &enc, &encop, &dec, &decop);
  int64_t bbnum = (tchdbbnum(tdb->hdb) / TDBIDXLMEMB) * 4 + TDBIDXLMEMB;
  uint8_t opts = tdb->opts;
  uint8_t bopts = 0;
  if(opts & TDBTLARGE) bopts |= BDBTLARGE;
  if(opts & TDBTDEFLATE) bopts |= BDBTDEFLATE;
  if(opts & TDBTBZIP) bopts |= BDBTBZIP;
  if(opts & TDBTTCBS) bopts |= BDBTTCBS;
  if(opts & TDBTEXCODEC) bopts |= BDBTEXCODEC;
  switch(type){
  case TDBITLEXICAL:
    idx->db = tcbdbnew();
    idx->name = tcstrdup(name);
    tcxstrprintf(pbuf, "%clex", MYEXTCHR);
    if(dbgfd >= 0) tcbdbsetdbgfd(idx->db, dbgfd);
    if(tdb->mmtx) tcbdbsetmutex(idx->db);
    if(enc && dec) tcbdbsetcodecfunc(idx->db, enc, encop, dec, decop);
    tcbdbtune(idx->db, TDBIDXLMEMB, TDBIDXNMEMB, bbnum, -1, -1, bopts);
    tcbdbsetcache(idx->db, tdb->lcnum, tdb->ncnum);
    tcbdbsetdfunit(idx->db, tchdbdfunit(tdb->hdb));
    tcbdbsetlsmax(idx->db, TDBIDXLSMAX);
    if(!tcbdbopen(idx->db, TCXSTRPTR(pbuf), bomode)){
      tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
      err = true;
    }
    tdb->inum++;
    break;
  case TDBITDECIMAL:
    idx->db = tcbdbnew();
    idx->name = tcstrdup(name);
    tcxstrprintf(pbuf, "%cdec", MYEXTCHR);
    if(dbgfd >= 0) tcbdbsetdbgfd(idx->db, dbgfd);
    if(tdb->mmtx) tcbdbsetmutex(idx->db);
    tcbdbsetcmpfunc(idx->db, tccmpdecimal, NULL);
    if(enc && dec) tcbdbsetcodecfunc(idx->db, enc, encop, dec, decop);
    tcbdbtune(idx->db, TDBIDXLMEMB, TDBIDXNMEMB, bbnum, -1, -1, bopts);
    tcbdbsetcache(idx->db, tdb->lcnum, tdb->ncnum);
    tcbdbsetdfunit(idx->db, tchdbdfunit(tdb->hdb));
    tcbdbsetlsmax(idx->db, TDBIDXLSMAX);
    if(!tcbdbopen(idx->db, TCXSTRPTR(pbuf), bomode)){
      tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
      err = true;
    }
    tdb->inum++;
    break;
  case TDBITTOKEN:
    idx->db = tcbdbnew();
    idx->cc = tcmapnew2(TDBIDXCCBNUM);
    idx->name = tcstrdup(name);
    tcxstrprintf(pbuf, "%ctok", MYEXTCHR);
    if(dbgfd >= 0) tcbdbsetdbgfd(idx->db, dbgfd);
    if(tdb->mmtx) tcbdbsetmutex(idx->db);
    if(enc && dec) tcbdbsetcodecfunc(idx->db, enc, encop, dec, decop);
    tcbdbtune(idx->db, TDBIDXLMEMB, TDBIDXNMEMB, bbnum, -1, -1, bopts);
    tcbdbsetcache(idx->db, tdb->lcnum, tdb->ncnum);
    tcbdbsetdfunit(idx->db, tchdbdfunit(tdb->hdb));
    tcbdbsetlsmax(idx->db, TDBIDXLSMAX);
    if(!tcbdbopen(idx->db, TCXSTRPTR(pbuf), bomode)){
      tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
      err = true;
    }
    tdb->inum++;
    break;
  default:
    tctdbsetecode(tdb, TCEINVALID, __FILE__, __LINE__, __func__);
    err = true;
    break;
  }
  idx->type = type;
  if(!err){
    TCHDB *hdb = tdb->hdb;
    if(!tchdbiterinit(hdb)) err = true;
    void *db = idx->db;
    TCXSTR *kxstr = tcxstrnew();
    TCXSTR *vxstr = tcxstrnew();
    int nsiz = strlen(name);
    while(tchdbiternext3(hdb, kxstr, vxstr)){
      if(nsiz < 1){
        const char *pkbuf = TCXSTRPTR(kxstr);
        int pksiz = TCXSTRSIZE(kxstr);
        switch(type){
        case TDBITLEXICAL:
        case TDBITDECIMAL:
          if(!tcbdbput(db, pkbuf, pksiz, pkbuf, pksiz)){
            tctdbsetecode(tdb, tcbdbecode(db), __FILE__, __LINE__, __func__);
            err = true;
          }
          break;
        case TDBITTOKEN:
          if(!tctdbidxputtoken(tdb, idx, pkbuf, pksiz, pkbuf, pksiz)) err = true;
          break;
        }
      } else {
        const char *pkbuf = TCXSTRPTR(kxstr);
        int pksiz = TCXSTRSIZE(kxstr);
        uint16_t hash = tctdbidxhash(pkbuf, pksiz);
        int vsiz;
        char *vbuf = tcmaploadone(TCXSTRPTR(vxstr), TCXSTRSIZE(vxstr), name, nsiz, &vsiz);
        if(vbuf){
          switch(type){
          case TDBITLEXICAL:
          case TDBITDECIMAL:
            if(!tctdbidxputone(tdb, idx, pkbuf, pksiz, hash, vbuf, vsiz)) err = true;
            break;
          case TDBITTOKEN:
            if(!tctdbidxputtoken(tdb, idx, pkbuf, pksiz, vbuf, vsiz)) err = true;
            break;
          }
          TCFREE(vbuf);
        }
      }
    }
    tcxstrdel(vxstr);
    tcxstrdel(kxstr);
  }
  tcxstrdel(pbuf);
  return !err;
}


/* Generate a unique ID number.
   `tdb' specifies the table database object.
   `inc' specifies the increment of the seed.
   The return value is the new unique ID number or -1 on failure. */
static int64_t tctdbgenuidimpl(TCTDB *tdb, int64_t inc){
  assert(tdb);
  void *opq = tchdbopaque(tdb->hdb);
  uint64_t llnum, uid;
  if(inc < 0){
    uid = -inc - 1;
  } else {
    memcpy(&llnum, opq, sizeof(llnum));
    if(inc == 0) return TCITOHLL(llnum);
    uid = TCITOHLL(llnum) + inc;
  }
  llnum = TCITOHLL(uid);
  memcpy(opq, &llnum, sizeof(llnum));
  return uid;
}


/* Execute the search of a query object.
   `qry' specifies the query object.
   The return value is a list object of the primary keys of the corresponding records. */
static TCLIST *tctdbqrysearchimpl(TDBQRY *qry){
  assert(qry);
  TCTDB *tdb = qry->tdb;
  TCHDB *hdb = tdb->hdb;
  TDBIDX *idxs = tdb->idxs;
  int inum = tdb->inum;
  TDBCOND *conds = qry->conds;
  int cnum = qry->cnum;
  int acnum = cnum;
  int max = qry->max;
  if(max < INT_MAX - qry->skip) max += qry->skip;
  const char *oname = qry->oname;
  int otype = qry->otype;
  TCXSTR *hint = qry->hint;
  TCLIST *res = NULL;
  for(int i = 0; i < cnum; i++){
    TDBCOND *cond = conds + i;
    cond->alive = true;
  }
  tcxstrclear(hint);
  bool isord = oname != NULL;
  TDBCOND *mcond = NULL;
  TDBIDX *midx = NULL;
  TDBCOND *ncond = NULL;
  TDBIDX *nidx = NULL;
  TDBCOND *scond = NULL;
  TDBIDX *sidx = NULL;
  for(int i = 0; i < cnum; i++){
    TDBCOND *cond = conds + i;
    if(!cond->sign || cond->noidx) continue;
    for(int j = 0; j < inum; j++){
      TDBIDX *idx = idxs + j;
      if(!strcmp(cond->name, idx->name)){
        switch(idx->type){
        case TDBITLEXICAL:
          switch(cond->op){
          case TDBQCSTREQ:
          case TDBQCSTRBW:
          case TDBQCSTROREQ:
            if(!mcond){
              mcond = cond;
              midx = idx;
            } else if(!ncond){
              ncond = cond;
              nidx = idx;
            }
            break;
          default:
            if(!scond){
              scond = cond;
              sidx = idx;
            }
            break;
          }
          break;
        case TDBITDECIMAL:
          switch(cond->op){
          case TDBQCNUMEQ:
          case TDBQCNUMGT:
          case TDBQCNUMGE:
          case TDBQCNUMLT:
          case TDBQCNUMLE:
          case TDBQCNUMBT:
          case TDBQCNUMOREQ:
            if(!mcond){
              mcond = cond;
              midx = idx;
            } else if(!ncond){
              ncond = cond;
              nidx = idx;
            }
            break;
          default:
            if(!scond){
              scond = cond;
              sidx = idx;
            }
            break;
          }
          break;
        case TDBITTOKEN:
          switch(cond->op){
          case TDBQCSTRAND:
          case TDBQCSTROR:
            if(!mcond){
              mcond = cond;
              midx = idx;
            } else if(!ncond){
              ncond = cond;
              nidx = idx;
            }
            break;
          }
          break;
        }
      }
    }
  }
  if(mcond){
    res = tclistnew();
    mcond->alive = false;
    acnum--;
    TCMAP *nmap = NULL;
    if(ncond){
      ncond->alive = false;
      acnum--;
      nmap = tctdbqryidxfetch(qry, ncond, nidx);
      max = tclmin(max, tcmaprnum(nmap));
    }
    const char *expr = mcond->expr;
    int esiz = mcond->esiz;
    TDBCOND *ucond = NULL;
    for(int i = 0; i < cnum; i++){
      TDBCOND *cond = conds + i;
      if(!cond->alive) continue;
      if(ucond){
        ucond = NULL;
        break;
      }
      ucond = cond;
    }
    bool trim = *midx->name != '\0';
    TCLIST *tokens;
    TCMAP *tres;
    BDBCUR *cur;
    const char *kbuf, *pv;
    char numbuf[TCNUMBUFSIZ];
    int ksiz, nsiz, tnum;
    int64_t xnum, lower, upper;
    bool all;
    switch(mcond->op){
    case TDBQCSTREQ:
      tcxstrprintf(hint, "using an index: \"%s\" asc (STREQ)\n", mcond->name);
      cur = tcbdbcurnew(midx->db);
      tcbdbcurjump(cur, expr, esiz + trim);
      if(oname && !strcmp(oname, mcond->name)) oname = NULL;
      all = oname != NULL;
      if(!all && max < INT_MAX) tcxstrprintf(hint, "limited matching: %d\n", max);
      trim = *midx->name != '\0';
      while((all || TCLISTNUM(res) < max) && (kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
        if(trim) ksiz -= 3;
        if(ksiz == esiz && !memcmp(kbuf, expr, esiz)){
          int vsiz;
          const char *vbuf = tcbdbcurval3(cur, &vsiz);
          if(!nmap || tcmapget(nmap, vbuf, vsiz, &nsiz)){
            if(acnum < 1){
              TCLISTPUSH(res, vbuf, vsiz);
            } else if(ucond){
              if(tctdbqryonecondmatch(qry, ucond, vbuf, vsiz)) TCLISTPUSH(res, vbuf, vsiz);
            } else if(tctdbqryallcondmatch(qry, vbuf, vsiz)){
              TCLISTPUSH(res, vbuf, vsiz);
            }
          }
        } else {
          break;
        }
        tcbdbcurnext(cur);
      }
      tcbdbcurdel(cur);
      break;
    case TDBQCSTRBW:
      tcxstrprintf(hint, "using an index: \"%s\" asc (STRBW)\n", mcond->name);
      cur = tcbdbcurnew(midx->db);
      tcbdbcurjump(cur, expr, esiz + trim);
      all = oname && (strcmp(oname, mcond->name) || otype != TDBQOSTRASC);
      if(!all && max < INT_MAX) tcxstrprintf(hint, "limited matching: %d\n", max);
      while((all || TCLISTNUM(res) < max) && (kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
        if(trim) ksiz -= 3;
        if(ksiz >= esiz && !memcmp(kbuf, expr, esiz)){
          int vsiz;
          const char *vbuf = tcbdbcurval3(cur, &vsiz);
          if(!nmap || tcmapget(nmap, vbuf, vsiz, &nsiz)){
            if(acnum < 1){
              TCLISTPUSH(res, vbuf, vsiz);
            } else if(ucond){
              if(tctdbqryonecondmatch(qry, ucond, vbuf, vsiz)) TCLISTPUSH(res, vbuf, vsiz);
            } else if(tctdbqryallcondmatch(qry, vbuf, vsiz)){
              TCLISTPUSH(res, vbuf, vsiz);
            }
          }
        } else {
          break;
        }
        tcbdbcurnext(cur);
      }
      tcbdbcurdel(cur);
      if(oname && !strcmp(oname, mcond->name)){
        if(otype == TDBQOSTRASC){
          oname = NULL;
        } else if(otype == TDBQOSTRDESC){
          tclistinvert(res);
          oname = NULL;
        }
      }
      break;
    case TDBQCSTROREQ:
      tcxstrprintf(hint, "using an index: \"%s\" skip (STROREQ)\n", mcond->name);
      cur = tcbdbcurnew(midx->db);
      tokens = tcstrsplit(expr, "\t\n\r ,");
      tclistsort(tokens);
      for(int i = 1; i < TCLISTNUM(tokens); i++){
        if(!strcmp(TCLISTVALPTR(tokens, i), TCLISTVALPTR(tokens, i - 1))){
          TCFREE(tclistremove2(tokens, i));
          i--;
        }
      }
      if(oname && !strcmp(oname, mcond->name)){
        if(otype == TDBQOSTRASC){
          oname = NULL;
        } else if(otype == TDBQOSTRDESC){
          tclistinvert(tokens);
          oname = NULL;
        }
      }
      tnum = TCLISTNUM(tokens);
      all = oname != NULL;
      if(!all && max < INT_MAX) tcxstrprintf(hint, "limited matching: %d\n", max);
      for(int i = 0; (all || TCLISTNUM(res) < max) && i < tnum; i++){
        const char *token;
        int tsiz;
        TCLISTVAL(token, tokens, i, tsiz);
        if(tsiz < 1) continue;
        tcbdbcurjump(cur, token, tsiz + trim);
        while((all || TCLISTNUM(res) < max) && (kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
          if(trim) ksiz -= 3;
          if(ksiz == tsiz && !memcmp(kbuf, token, tsiz)){
            int vsiz;
            const char *vbuf = tcbdbcurval3(cur, &vsiz);
            if(!nmap || tcmapget(nmap, vbuf, vsiz, &nsiz)){
              if(acnum < 1){
                TCLISTPUSH(res, vbuf, vsiz);
              } else if(ucond){
                if(tctdbqryonecondmatch(qry, ucond, vbuf, vsiz)) TCLISTPUSH(res, vbuf, vsiz);
              } else if(tctdbqryallcondmatch(qry, vbuf, vsiz)){
                TCLISTPUSH(res, vbuf, vsiz);
              }
            }
          } else {
            break;
          }
          tcbdbcurnext(cur);
        }
      }
      tclistdel(tokens);
      tcbdbcurdel(cur);
      break;
    case TDBQCNUMEQ:
      tcxstrprintf(hint, "using an index: \"%s\" asc (NUMEQ)\n", mcond->name);
      cur = tcbdbcurnew(midx->db);
      if(oname && !strcmp(oname, mcond->name)) oname = NULL;
      xnum = tcatoi(expr);
      ksiz = sprintf(numbuf, "\x01%lld", (long long)xnum);
      tcbdbcurjump(cur, numbuf, ksiz);
      all = oname != NULL;
      if(!all && max < INT_MAX) tcxstrprintf(hint, "limited matching: %d\n", max);
      while((all || TCLISTNUM(res) < max) && (kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
        if(tcatoi(kbuf) == xnum){
          int vsiz;
          const char *vbuf = tcbdbcurval3(cur, &vsiz);
          if(!nmap || tcmapget(nmap, vbuf, vsiz, &nsiz)){
            if(acnum < 1){
              TCLISTPUSH(res, vbuf, vsiz);
            } else if(ucond){
              if(tctdbqryonecondmatch(qry, ucond, vbuf, vsiz)) TCLISTPUSH(res, vbuf, vsiz);
            } else if(tctdbqryallcondmatch(qry, vbuf, vsiz)){
              TCLISTPUSH(res, vbuf, vsiz);
            }
          }
        } else {
          break;
        }
        tcbdbcurnext(cur);
      }
      tcbdbcurdel(cur);
      break;
    case TDBQCNUMGT:
    case TDBQCNUMGE:
      if(oname && !strcmp(oname, mcond->name) && otype == TDBQONUMDESC){
        tcxstrprintf(hint, "using an index: \"%s\" desc (NUMGT/NUMGE)\n", mcond->name);
        xnum = tcatoi(expr);
        cur = tcbdbcurnew(midx->db);
        tcbdbcurlast(cur);
        if(max < INT_MAX) tcxstrprintf(hint, "limited matching: %d\n", max);
        while(TCLISTNUM(res) < max && (kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
          int64_t knum = tcatoi(kbuf);
          if(knum < xnum) break;
          if(mcond->op == TDBQCNUMGE || tcatoi(kbuf) > xnum){
            int vsiz;
            const char *vbuf = tcbdbcurval3(cur, &vsiz);
            if(!nmap || tcmapget(nmap, vbuf, vsiz, &nsiz)){
              if(acnum < 1){
                TCLISTPUSH(res, vbuf, vsiz);
              } else if(ucond){
                if(tctdbqryonecondmatch(qry, ucond, vbuf, vsiz)) TCLISTPUSH(res, vbuf, vsiz);
              } else if(tctdbqryallcondmatch(qry, vbuf, vsiz)){
                TCLISTPUSH(res, vbuf, vsiz);
              }
            }
          }
          tcbdbcurprev(cur);
        }
        tcbdbcurdel(cur);
        oname = NULL;
      } else {
        tcxstrprintf(hint, "using an index: \"%s\" asc (NUMGT/NUMGE)\n", mcond->name);
        xnum = tcatoi(expr);
        cur = tcbdbcurnew(midx->db);
        ksiz = sprintf(numbuf, "\x01%lld", (long long)xnum);
        tcbdbcurjump(cur, numbuf, ksiz);
        all = oname && (strcmp(oname, mcond->name) || otype != TDBQONUMASC);
        if(!all && max < INT_MAX) tcxstrprintf(hint, "limited matching: %d\n", max);
        while((all || TCLISTNUM(res) < max) && (kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
          if(mcond->op == TDBQCNUMGE || tcatoi(kbuf) > xnum){
            int vsiz;
            const char *vbuf = tcbdbcurval3(cur, &vsiz);
            if(!nmap || tcmapget(nmap, vbuf, vsiz, &nsiz)){
              if(acnum < 1){
                TCLISTPUSH(res, vbuf, vsiz);
              } else if(ucond){
                if(tctdbqryonecondmatch(qry, ucond, vbuf, vsiz)) TCLISTPUSH(res, vbuf, vsiz);
              } else if(tctdbqryallcondmatch(qry, vbuf, vsiz)){
                TCLISTPUSH(res, vbuf, vsiz);
              }
            }
          }
          tcbdbcurnext(cur);
        }
        tcbdbcurdel(cur);
        if(!all) oname = NULL;
      }
      break;
    case TDBQCNUMLT:
    case TDBQCNUMLE:
      if(oname && !strcmp(oname, mcond->name) && otype == TDBQONUMASC){
        tcxstrprintf(hint, "using an index: \"%s\" asc (NUMLT/NUMLE)\n", mcond->name);
        xnum = tcatoi(expr);
        cur = tcbdbcurnew(midx->db);
        tcbdbcurfirst(cur);
        if(max < INT_MAX) tcxstrprintf(hint, "limited matching: %d\n", max);
        while(TCLISTNUM(res) < max && (kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
          int64_t knum = tcatoi(kbuf);
          if(knum > xnum) break;
          if(mcond->op == TDBQCNUMLE || tcatoi(kbuf) < xnum){
            int vsiz;
            const char *vbuf = tcbdbcurval3(cur, &vsiz);
            if(!nmap || tcmapget(nmap, vbuf, vsiz, &nsiz)){
              if(acnum < 1){
                TCLISTPUSH(res, vbuf, vsiz);
              } else if(ucond){
                if(tctdbqryonecondmatch(qry, ucond, vbuf, vsiz)) TCLISTPUSH(res, vbuf, vsiz);
              } else if(tctdbqryallcondmatch(qry, vbuf, vsiz)){
                TCLISTPUSH(res, vbuf, vsiz);
              }
            }
          }
          tcbdbcurnext(cur);
        }
        tcbdbcurdel(cur);
        oname = NULL;
      } else {
        tcxstrprintf(hint, "using an index: \"%s\" desc (NUMLT/NUMLE)\n", mcond->name);
        xnum = tcatoi(expr);
        cur = tcbdbcurnew(midx->db);
        ksiz = sprintf(numbuf, "\x7f%lld", (long long)xnum);
        tcbdbcurjumpback(cur, numbuf, ksiz);
        all = oname && (strcmp(oname, mcond->name) || otype != TDBQONUMDESC);
        if(!all && max < INT_MAX) tcxstrprintf(hint, "limited matching: %d\n", max);
        while((all || TCLISTNUM(res) < max) && (kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
          if(mcond->op == TDBQCNUMLE || tcatoi(kbuf) < xnum){
            int vsiz;
            const char *vbuf = tcbdbcurval3(cur, &vsiz);
            if(!nmap || tcmapget(nmap, vbuf, vsiz, &nsiz)){
              if(acnum < 1){
                TCLISTPUSH(res, vbuf, vsiz);
              } else if(ucond){
                if(tctdbqryonecondmatch(qry, ucond, vbuf, vsiz)) TCLISTPUSH(res, vbuf, vsiz);
              } else if(tctdbqryallcondmatch(qry, vbuf, vsiz)){
                TCLISTPUSH(res, vbuf, vsiz);
              }
            }
          }
          tcbdbcurprev(cur);
        }
        tcbdbcurdel(cur);
        if(!all) oname = NULL;
      }
      break;
    case TDBQCNUMBT:
      tcxstrprintf(hint, "using an index: \"%s\" asc (NUMBT)\n", mcond->name);
      xnum = tcatoi(expr);
      while(*expr == ' ' || *expr == ','){
        expr++;
      }
      pv = expr;
      while(*pv != '\0' && *pv != ' ' && *pv != ','){
        pv++;
      }
      if(*pv != ' ' && *pv != ',') pv = " ";
      pv++;
      while(*pv == ' ' || *pv == ','){
        pv++;
      }
      lower = tcatoi(expr);
      upper = tcatoi(pv);
      if(lower > upper){
        int64_t swap = lower;
        lower = upper;
        upper = swap;
      }
      cur = tcbdbcurnew(midx->db);
      ksiz = sprintf(numbuf, "\x01%lld", (long long)lower);
      tcbdbcurjump(cur, numbuf, ksiz);
      all = oname && (strcmp(oname, mcond->name) || otype != TDBQONUMASC);
      if(!all && max < INT_MAX) tcxstrprintf(hint, "limited matching: %d\n", max);
      while((all || TCLISTNUM(res) < max) && (kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
        if(tcatoi(kbuf) > upper) break;
        int vsiz;
        const char *vbuf = tcbdbcurval3(cur, &vsiz);
        if(!nmap || tcmapget(nmap, vbuf, vsiz, &nsiz)){
          if(acnum < 1){
            TCLISTPUSH(res, vbuf, vsiz);
          } else if(ucond){
            if(tctdbqryonecondmatch(qry, ucond, vbuf, vsiz)) TCLISTPUSH(res, vbuf, vsiz);
          } else if(tctdbqryallcondmatch(qry, vbuf, vsiz)){
            TCLISTPUSH(res, vbuf, vsiz);
          }
        }
        tcbdbcurnext(cur);
      }
      tcbdbcurdel(cur);
      if(oname && !strcmp(oname, mcond->name)){
        if(otype == TDBQONUMASC){
          oname = NULL;
        } else if(otype == TDBQONUMDESC){
          tclistinvert(res);
          oname = NULL;
        }
      }
      break;
    case TDBQCNUMOREQ:
      tcxstrprintf(hint, "using an index: \"%s\" skip (NUMOREQ)\n", mcond->name);
      cur = tcbdbcurnew(midx->db);
      tokens = tcstrsplit(expr, "\t\n\r ,");
      tclistsortex(tokens, tdbcmppkeynumasc);
      for(int i = 1; i < TCLISTNUM(tokens); i++){
        if(tcatoi(TCLISTVALPTR(tokens, i)) == tcatoi(TCLISTVALPTR(tokens, i - 1))){
          TCFREE(tclistremove2(tokens, i));
          i--;
        }
      }
      if(oname && !strcmp(oname, mcond->name)){
        if(otype == TDBQONUMASC){
          oname = NULL;
        } else if(otype == TDBQONUMDESC){
          tclistinvert(tokens);
          oname = NULL;
        }
      }
      tnum = TCLISTNUM(tokens);
      all = oname != NULL;
      if(!all && max < INT_MAX) tcxstrprintf(hint, "limited matching: %d\n", max);
      for(int i = 0; (all || TCLISTNUM(res) < max) && i < tnum; i++){
        const char *token;
        int tsiz;
        TCLISTVAL(token, tokens, i, tsiz);
        if(tsiz < 1) continue;
        xnum = tcatoi(token);
        ksiz = sprintf(numbuf, "\x01%lld", (long long)xnum);
        tcbdbcurjump(cur, numbuf, ksiz);
        while((all || TCLISTNUM(res) < max) && (kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
          if(tcatoi(kbuf) == xnum){
            int vsiz;
            const char *vbuf = tcbdbcurval3(cur, &vsiz);
            if(!nmap || tcmapget(nmap, vbuf, vsiz, &nsiz)){
              if(acnum < 1){
                TCLISTPUSH(res, vbuf, vsiz);
              } else if(ucond){
                if(tctdbqryonecondmatch(qry, ucond, vbuf, vsiz)) TCLISTPUSH(res, vbuf, vsiz);
              } else if(tctdbqryallcondmatch(qry, vbuf, vsiz)){
                TCLISTPUSH(res, vbuf, vsiz);
              }
            }
          } else {
            break;
          }
          tcbdbcurnext(cur);
        }
      }
      tclistdel(tokens);
      tcbdbcurdel(cur);
      break;
    case TDBQCSTRAND:
    case TDBQCSTROR:
      tcxstrprintf(hint, "using an index: \"%s\" inverted (%s)\n",
                   mcond->name, mcond->op == TDBQCSTRAND ? "STRAND" : "STROR");
      all = oname != NULL;
      if(!all && max < INT_MAX) tcxstrprintf(hint, "limited matching: %d\n", max);
      tokens = tcstrsplit(expr, "\t\n\r ,");
      tclistsort(tokens);
      for(int i = 1; i < TCLISTNUM(tokens); i++){
        if(!strcmp(TCLISTVALPTR(tokens, i), TCLISTVALPTR(tokens, i - 1))){
          TCFREE(tclistremove2(tokens, i));
          i--;
        }
      }
      tres = tctdbidxgetbytokens(tdb, midx, tokens, mcond->op);
      tcmapiterinit(tres);
      while((all || TCLISTNUM(res) < max) && (kbuf = tcmapiternext(tres, &ksiz)) != NULL){
        if(!nmap || tcmapget(nmap, kbuf, ksiz, &nsiz)){
          if(acnum < 1){
            TCLISTPUSH(res, kbuf, ksiz);
          } else if(ucond){
            if(tctdbqryonecondmatch(qry, ucond, kbuf, ksiz)) TCLISTPUSH(res, kbuf, ksiz);
          } else if(tctdbqryallcondmatch(qry, kbuf, ksiz)){
            TCLISTPUSH(res, kbuf, ksiz);
          }
        }
      }
      tcmapdel(tres);
      tclistdel(tokens);
      break;
    }
    if(nmap) tcmapdel(nmap);
  }
  if(!res && scond){
    res = tclistnew();
    scond->alive = false;
    acnum--;
    int op = scond->op;
    const char *expr = scond->expr;
    int esiz = scond->esiz;
    TDBCOND *ucond = NULL;
    for(int i = 0; i < cnum; i++){
      TDBCOND *cond = conds + i;
      if(!cond->alive) continue;
      if(ucond){
        ucond = NULL;
        break;
      }
      ucond = cond;
    }
    bool trim = *sidx->name != '\0';
    bool asc = true;
    bool all = true;
    if(!oname){
      all = false;
    } else if(!strcmp(oname, scond->name)){
      switch(sidx->type){
      case TDBITLEXICAL:
        switch(otype){
        case TDBQOSTRASC:
          asc = true;
          all = false;
          break;
        case TDBQOSTRDESC:
          asc = false;
          all = false;
          break;
        }
        break;
      case TDBITDECIMAL:
        switch(otype){
        case TDBQONUMASC:
          asc = true;
          all = false;
          break;
        case TDBQONUMDESC:
          asc = false;
          all = false;
          break;
        }
        break;
      }
    }
    if(asc){
      tcxstrprintf(hint, "using an index: \"%s\" asc (all)\n", scond->name);
      BDBCUR *cur = tcbdbcurnew(sidx->db);
      tcbdbcurfirst(cur);
      if(!all && max < INT_MAX) tcxstrprintf(hint, "limited matching: %d\n", max);
      const char *kbuf;
      int ksiz;
      while((all || TCLISTNUM(res) < max) && (kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
        if(trim) ksiz -= 3;
        if(ksiz < 0) break;
        if(tctdbqrycondmatch(op, expr, esiz, kbuf, ksiz)){
          int vsiz;
          const char *vbuf = tcbdbcurval3(cur, &vsiz);
          if(acnum < 1){
            TCLISTPUSH(res, vbuf, vsiz);
          } else if(ucond){
            if(tctdbqryonecondmatch(qry, ucond, vbuf, vsiz)) TCLISTPUSH(res, vbuf, vsiz);
          } else if(tctdbqryallcondmatch(qry, vbuf, vsiz)){
            TCLISTPUSH(res, vbuf, vsiz);
          }
        }
        tcbdbcurnext(cur);
      }
      tcbdbcurdel(cur);
    } else {
      tcxstrprintf(hint, "using an index: \"%s\" desc (all)\n", scond->name);
      BDBCUR *cur = tcbdbcurnew(sidx->db);
      tcbdbcurlast(cur);
      if(!all && max < INT_MAX) tcxstrprintf(hint, "limited matching: %d\n", max);
      const char *kbuf;
      int ksiz;
      while((all || TCLISTNUM(res) < max) && (kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
        if(trim) ksiz -= 3;
        if(ksiz < 0) break;
        if(tctdbqrycondmatch(op, expr, esiz, kbuf, ksiz)){
          int vsiz;
          const char *vbuf = tcbdbcurval3(cur, &vsiz);
          if(acnum < 1){
            TCLISTPUSH(res, vbuf, vsiz);
          } else if(ucond){
            if(tctdbqryonecondmatch(qry, ucond, vbuf, vsiz)) TCLISTPUSH(res, vbuf, vsiz);
          } else if(tctdbqryallcondmatch(qry, vbuf, vsiz)){
            TCLISTPUSH(res, vbuf, vsiz);
          }
        }
        tcbdbcurprev(cur);
      }
      tcbdbcurdel(cur);
    }
    if(!all) oname = NULL;
  }
  if(!res && oname && max < tchdbrnum(hdb) * TDBORDRATIO){
    TDBIDX *oidx = NULL;
    bool asc = true;
    for(int i = 0; !oidx && i < inum; i++){
      TDBIDX *idx = idxs + i;
      if(strcmp(idx->name, oname)) continue;
      switch(idx->type){
      case TDBITLEXICAL:
        switch(otype){
        case TDBQOSTRASC:
          oidx = idx;
          asc = true;
          break;
        case TDBQOSTRDESC:
          oidx = idx;
          asc = false;
          break;
        }
        break;
      case TDBITDECIMAL:
        switch(otype){
        case TDBQONUMASC:
          oidx = idx;
          asc = true;
          break;
        case TDBQONUMDESC:
          oidx = idx;
          asc = false;
          break;
        }
        break;
      }
    }
    if(oidx){
      res = tclistnew();
      TDBCOND *ucond = NULL;
      for(int i = 0; i < cnum; i++){
        TDBCOND *cond = conds + i;
        if(!cond->alive) continue;
        if(ucond){
          ucond = NULL;
          break;
        }
        ucond = cond;
      }
      bool trim = *oidx->name != '\0';
      if(asc){
        tcxstrprintf(hint, "using an index: \"%s\" asc (order)\n", oname);
        BDBCUR *cur = tcbdbcurnew(oidx->db);
        tcbdbcurfirst(cur);
        tcxstrprintf(hint, "limited matching: %d\n", max);
        const char *kbuf;
        int ksiz;
        while(TCLISTNUM(res) < max && (kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
          if(trim) ksiz -= 3;
          if(ksiz < 0) break;
          int vsiz;
          const char *vbuf = tcbdbcurval3(cur, &vsiz);
          if(acnum < 1){
            TCLISTPUSH(res, vbuf, vsiz);
          } else if(ucond){
            if(tctdbqryonecondmatch(qry, ucond, vbuf, vsiz)) TCLISTPUSH(res, vbuf, vsiz);
          } else if(tctdbqryallcondmatch(qry, vbuf, vsiz)){
            TCLISTPUSH(res, vbuf, vsiz);
          }
          tcbdbcurnext(cur);
        }
        tcbdbcurdel(cur);
      } else {
        tcxstrprintf(hint, "using an index: \"%s\" desc (order)\n", oname);
        BDBCUR *cur = tcbdbcurnew(oidx->db);
        tcbdbcurlast(cur);
        tcxstrprintf(hint, "limited matching: %d\n", max);
        const char *kbuf;
        int ksiz;
        while(TCLISTNUM(res) < max && (kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
          if(trim) ksiz -= 3;
          if(ksiz < 0) break;
          int vsiz;
          const char *vbuf = tcbdbcurval3(cur, &vsiz);
          if(acnum < 1){
            TCLISTPUSH(res, vbuf, vsiz);
          } else if(ucond){
            if(tctdbqryonecondmatch(qry, ucond, vbuf, vsiz)) TCLISTPUSH(res, vbuf, vsiz);
          } else if(tctdbqryallcondmatch(qry, vbuf, vsiz)){
            TCLISTPUSH(res, vbuf, vsiz);
          }
          tcbdbcurprev(cur);
        }
        tcbdbcurdel(cur);
      }
      int rnum = TCLISTNUM(res);
      if(rnum >= max || tcbdbrnum(oidx->db) >= tchdbrnum(hdb)){
        oname = NULL;
      } else {
        tcxstrprintf(hint, "abort the result: %d\n", rnum);
        tclistdel(res);
        res = NULL;
      }
    }
  }
  if(!res){
    tcxstrprintf(hint, "scanning the whole table\n");
    res = tclistnew();
    TDBCOND *ucond = NULL;
    for(int i = 0; i < cnum; i++){
      TDBCOND *cond = conds + i;
      if(!cond->alive) continue;
      if(ucond){
        ucond = NULL;
        break;
      }
      ucond = cond;
    }
    char *lkbuf = NULL;
    int lksiz = 0;
    char *pkbuf;
    int pksiz;
    const char *cbuf;
    int csiz;
    bool all = oname != NULL;
    if(!all && max < INT_MAX) tcxstrprintf(hint, "limited matching: %d\n", max);
    while((all || TCLISTNUM(res) < max) &&
          (pkbuf = tchdbgetnext3(hdb, lkbuf, lksiz, &pksiz, &cbuf, &csiz)) != NULL){
      if(ucond){
        if(ucond->nsiz < 1){
          char *tkbuf;
          TCMEMDUP(tkbuf, pkbuf, pksiz);
          if(tctdbqrycondmatch(ucond->op, ucond->expr, ucond->esiz, tkbuf, pksiz) == ucond->sign)
            TCLISTPUSH(res, pkbuf, pksiz);
          TCFREE(tkbuf);
        } else {
          int vsiz;
          char *vbuf = tcmaploadone(cbuf, csiz, ucond->name, ucond->nsiz, &vsiz);
          if(vbuf){
            if(tctdbqrycondmatch(ucond->op, ucond->expr, ucond->esiz, vbuf, vsiz) == ucond->sign)
              TCLISTPUSH(res, pkbuf, pksiz);
            TCFREE(vbuf);
          } else {
            if(!ucond->sign) TCLISTPUSH(res, pkbuf, pksiz);
          }
        }
      } else {
        TCMAP *cols = tcmapload(cbuf, csiz);
        bool ok = true;
        for(int i = 0; i < cnum; i++){
          TDBCOND *cond = conds + i;
          if(cond->nsiz < 1){
            char *tkbuf;
            TCMEMDUP(tkbuf, pkbuf, pksiz);
            if(tctdbqrycondmatch(cond->op, cond->expr, cond->esiz, tkbuf, pksiz) != cond->sign){
              TCFREE(tkbuf);
              ok = false;
              break;
            }
            TCFREE(tkbuf);
          } else {
            int vsiz;
            const char *vbuf = tcmapget(cols, cond->name, cond->nsiz, &vsiz);
            if(vbuf){
              if(tctdbqrycondmatch(cond->op, cond->expr, cond->esiz, vbuf, vsiz) != cond->sign){
                ok = false;
                break;
              }
            } else {
              if(cond->sign){
                ok = false;
                break;
              }
            }
          }
        }
        if(ok) TCLISTPUSH(res, pkbuf, pksiz);
        tcmapdel(cols);
      }
      TCFREE(lkbuf);
      lkbuf = pkbuf;
      lksiz = pksiz;
    }
    TCFREE(lkbuf);
  }
  int rnum = TCLISTNUM(res);
  tcxstrprintf(hint, "result set size: %d\n", rnum);
  if(oname){
    if(*oname == '\0'){
      tcxstrprintf(hint, "sorting the result set: \"%s\"\n", oname);
      switch(otype){
      case TDBQOSTRASC:
        tclistsort(res);
        break;
      case TDBQOSTRDESC:
        tclistsort(res);
        tclistinvert(res);
        break;
      case TDBQONUMASC:
        tclistsortex(res, tdbcmppkeynumasc);
        break;
      case TDBQONUMDESC:
        tclistsortex(res, tdbcmppkeynumdesc);
        break;
      }
    } else {
      tcxstrprintf(hint, "sorting the result set: \"%s\"\n", oname);
      TDBSORTKEY *keys;
      TCMALLOC(keys, sizeof(*keys) * rnum + 1);
      int onsiz = strlen(oname);
      for(int i = 0; i < rnum; i++){
        TDBSORTKEY *key = keys + i;
        const char *kbuf;
        int ksiz;
        TCLISTVAL(kbuf, res, i, ksiz);
        char *vbuf = NULL;
        int vsiz = 0;
        int csiz;
        char *cbuf = tchdbget(hdb, kbuf, ksiz, &csiz);
        if(cbuf){
          vbuf = tcmaploadone(cbuf, csiz, oname, onsiz, &vsiz);
          TCFREE(cbuf);
        }
        key->kbuf = kbuf;
        key->ksiz = ksiz;
        key->vbuf = vbuf;
        key->vsiz = vsiz;
      }
      int (*compar)(const TDBSORTKEY *a, const TDBSORTKEY *b) = NULL;
      switch(otype){
      case TDBQOSTRASC:
        compar = tdbcmpsortkeystrasc;
        break;
      case TDBQOSTRDESC:
        compar = tdbcmpsortkeystrdesc;
        break;
      case TDBQONUMASC:
        compar = tdbcmpsortkeynumasc;
        break;
      case TDBQONUMDESC:
        compar = tdbcmpsortkeynumdesc;
        break;
      }
      if(compar){
        if(max <= rnum / 16){
          tctopsort(keys, rnum, sizeof(*keys), max, (int (*)(const void *, const void *))compar);
        } else {
          qsort(keys, rnum, sizeof(*keys), (int (*)(const void *, const void *))compar);
        }
      }
      TCLIST *nres = tclistnew2(rnum);
      for(int i = 0; i < rnum; i++){
        TDBSORTKEY *key = keys + i;
        TCLISTPUSH(nres, key->kbuf, key->ksiz);
        TCFREE(key->vbuf);
      }
      tclistdel(res);
      res = nres;
      TCFREE(keys);
    }
  } else if(isord){
    tcxstrprintf(hint, "leaving the index order\n");
  } else {
    tcxstrprintf(hint, "leaving the natural order\n");
  }
  if(qry->skip > 0){
    int left = tclmin(TCLISTNUM(res), qry->skip);
    while(left-- > 0){
      int rsiz;
      TCFREE(tclistshift(res, &rsiz));
    }
    max -= qry->skip;
  }
  if(TCLISTNUM(res) > max){
    int left = TCLISTNUM(res) - max;
    while(left-- > 0){
      int rsiz;
      TCFREE(tclistpop(res, &rsiz));
    }
  }
  qry->count = TCLISTNUM(res);
  return res;
}


/* Fetch record keys from an index matching to a condition.
   `qry' specifies the query object.
   `cond' specifies a condition object.
   `idx' specifies an index object.
   The return value is a map object containing primary keys of the corresponding records. */
static TCMAP *tctdbqryidxfetch(TDBQRY *qry, TDBCOND *cond, TDBIDX *idx){
  assert(qry && cond && idx);
  TCTDB *tdb = qry->tdb;
  TCHDB *hdb = tdb->hdb;
  TCXSTR *hint = qry->hint;
  const char *expr = cond->expr;
  int esiz = cond->esiz;
  bool trim = *idx->name != '\0';
  TCMAP *nmap = tcmapnew2(tclmin(TDBDEFBNUM, tchdbrnum(hdb)) / 4 + 1);
  TCLIST *tokens;
  BDBCUR *cur;
  const char *kbuf, *pv;
  char numbuf[TCNUMBUFSIZ];
  int ksiz, tnum;
  int64_t xnum, lower, upper;
  switch(cond->op){
  case TDBQCSTREQ:
    tcxstrprintf(hint, "using an auxiliary index: \"%s\" one (STREQ)\n", cond->name);
    cur = tcbdbcurnew(idx->db);
    tcbdbcurjump(cur, expr, esiz + trim);
    while((kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
      if(trim) ksiz -= 3;
      if(ksiz == esiz && !memcmp(kbuf, expr, esiz)){
        int vsiz;
        const char *vbuf = tcbdbcurval3(cur, &vsiz);
        tcmapputkeep(nmap, vbuf, vsiz, "", 0);
      } else {
        break;
      }
      tcbdbcurnext(cur);
    }
    tcbdbcurdel(cur);
    break;
  case TDBQCSTRBW:
    tcxstrprintf(hint, "using an auxiliary index: \"%s\" asc (STRBW)\n", cond->name);
    cur = tcbdbcurnew(idx->db);
    tcbdbcurjump(cur, expr, esiz + trim);
    while((kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
      if(trim) ksiz -= 3;
      if(ksiz >= esiz && !memcmp(kbuf, expr, esiz)){
        int vsiz;
        const char *vbuf = tcbdbcurval3(cur, &vsiz);
        tcmapputkeep(nmap, vbuf, vsiz, "", 0);
      } else {
        break;
      }
      tcbdbcurnext(cur);
    }
    tcbdbcurdel(cur);
    break;
  case TDBQCSTROREQ:
    tcxstrprintf(hint, "using an auxiliary index: \"%s\" skip (STROREQ)\n", cond->name);
    tokens = tcstrsplit(expr, "\t\n\r ,");
    tclistsort(tokens);
    for(int i = 1; i < TCLISTNUM(tokens); i++){
      if(!strcmp(TCLISTVALPTR(tokens, i), TCLISTVALPTR(tokens, i - 1))){
        TCFREE(tclistremove2(tokens, i));
        i--;
      }
    }
    tnum = TCLISTNUM(tokens);
    for(int i = 0; i < tnum; i++){
      const char *token;
      int tsiz;
      TCLISTVAL(token, tokens, i, tsiz);
      if(tsiz < 1) continue;
      cur = tcbdbcurnew(idx->db);
      tcbdbcurjump(cur, token, tsiz + trim);
      while((kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
        if(trim) ksiz -= 3;
        if(ksiz == tsiz && !memcmp(kbuf, token, tsiz)){
          int vsiz;
          const char *vbuf = tcbdbcurval3(cur, &vsiz);
          tcmapputkeep(nmap, vbuf, vsiz, "", 0);
        } else {
          break;
        }
        tcbdbcurnext(cur);
      }
      tcbdbcurdel(cur);
    }
    tclistdel(tokens);
    break;
  case TDBQCNUMEQ:
    tcxstrprintf(hint, "using an auxiliary index: \"%s\" asc (NUMEQ)\n", cond->name);
    xnum = tcatoi(expr);
    cur = tcbdbcurnew(idx->db);
    ksiz = sprintf(numbuf, "\x01%lld", (long long)xnum);
    tcbdbcurjump(cur, numbuf, ksiz);
    while((kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
      if(tcatoi(kbuf) == xnum){
        int vsiz;
        const char *vbuf = tcbdbcurval3(cur, &vsiz);
        tcmapputkeep(nmap, vbuf, vsiz, "", 0);
      } else {
        break;
      }
      tcbdbcurnext(cur);
    }
    tcbdbcurdel(cur);
    break;
  case TDBQCNUMGT:
  case TDBQCNUMGE:
    tcxstrprintf(hint, "using an auxiliary index: \"%s\" asc (NUMGT/NUMGE)\n", cond->name);
    xnum = tcatoi(expr);
    cur = tcbdbcurnew(idx->db);
    ksiz = sprintf(numbuf, "\x01%lld", (long long)xnum);
    tcbdbcurjump(cur, numbuf, ksiz);
    while((kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
      if(cond->op == TDBQCNUMGE || tcatoi(kbuf) > xnum){
        int vsiz;
        const char *vbuf = tcbdbcurval3(cur, &vsiz);
        tcmapputkeep(nmap, vbuf, vsiz, "", 0);
      }
      tcbdbcurnext(cur);
    }
    tcbdbcurdel(cur);
    break;
  case TDBQCNUMLT:
  case TDBQCNUMLE:
    tcxstrprintf(hint, "using an auxiliary index: \"%s\" desc (NUMLT/NUMLE)\n", cond->name);
    xnum = tcatoi(expr);
    cur = tcbdbcurnew(idx->db);
    ksiz = sprintf(numbuf, "\x7f%lld", (long long)xnum);
    tcbdbcurjumpback(cur, numbuf, ksiz);
    while((kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
      if(cond->op == TDBQCNUMLE || tcatoi(kbuf) < xnum){
        int vsiz;
        const char *vbuf = tcbdbcurval3(cur, &vsiz);
        tcmapputkeep(nmap, vbuf, vsiz, "", 0);
      }
      tcbdbcurprev(cur);
    }
    tcbdbcurdel(cur);
    break;
  case TDBQCNUMBT:
    tcxstrprintf(hint, "using an auxiliary index: \"%s\" asc (NUMBT)\n", cond->name);
    xnum = tcatoi(expr);
    while(*expr == ' ' || *expr == ','){
      expr++;
    }
    pv = expr;
    while(*pv != '\0' && *pv != ' ' && *pv != ','){
      pv++;
    }
    if(*pv != ' ' && *pv != ',') pv = " ";
    pv++;
    while(*pv == ' ' || *pv == ','){
      pv++;
    }
    lower = tcatoi(expr);
    upper = tcatoi(pv);
    if(lower > upper){
      int64_t swap = lower;
      lower = upper;
      upper = swap;
    }
    cur = tcbdbcurnew(idx->db);
    ksiz = sprintf(numbuf, "\x01%lld", (long long)lower);
    tcbdbcurjump(cur, numbuf, ksiz);
    while((kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
      if(tcatoi(kbuf) > upper) break;
      int vsiz;
      const char *vbuf = tcbdbcurval3(cur, &vsiz);
      tcmapputkeep(nmap, vbuf, vsiz, "", 0);
      tcbdbcurnext(cur);
    }
    tcbdbcurdel(cur);
    break;
  case TDBQCNUMOREQ:
    tcxstrprintf(hint, "using an auxiliary index: \"%s\" skip (NUMOREQ)\n", cond->name);
    cur = tcbdbcurnew(idx->db);
    tokens = tcstrsplit(expr, "\t\n\r ,");
    tclistsortex(tokens, tdbcmppkeynumasc);
    for(int i = 1; i < TCLISTNUM(tokens); i++){
      if(tcatoi(TCLISTVALPTR(tokens, i)) == tcatoi(TCLISTVALPTR(tokens, i - 1))){
        TCFREE(tclistremove2(tokens, i));
        i--;
      }
    }
    tnum = TCLISTNUM(tokens);
    for(int i = 0; i < tnum; i++){
      const char *token;
      int tsiz;
      TCLISTVAL(token, tokens, i, tsiz);
      if(tsiz < 1) continue;
      xnum = tcatoi(token);
      ksiz = sprintf(numbuf, "\x01%lld", (long long)xnum);
      tcbdbcurjump(cur, numbuf, ksiz);
      while((kbuf = tcbdbcurkey3(cur, &ksiz)) != NULL){
        if(tcatoi(kbuf) == xnum){
          int vsiz;
          const char *vbuf = tcbdbcurval3(cur, &vsiz);
          tcmapputkeep(nmap, vbuf, vsiz, "", 0);
        } else {
          break;
        }
        tcbdbcurnext(cur);
      }
    }
    tclistdel(tokens);
    tcbdbcurdel(cur);
    break;
  case TDBQCSTRAND:
  case TDBQCSTROR:
    tcxstrprintf(hint, "using an auxiliary index: \"%s\" inverted (%s)\n",
                 cond->name, cond->op == TDBQCSTRAND ? "STRAND" : "STROR");
    tokens = tcstrsplit(expr, "\t\n\r ,");
    tclistsort(tokens);
    for(int i = 1; i < TCLISTNUM(tokens); i++){
      if(!strcmp(TCLISTVALPTR(tokens, i), TCLISTVALPTR(tokens, i - 1))){
        TCFREE(tclistremove2(tokens, i));
        i--;
      }
    }
    tcmapdel(nmap);
    nmap = tctdbidxgetbytokens(tdb, idx, tokens, cond->op);
    tclistdel(tokens);
    break;
  }
  tcxstrprintf(hint, "auxiliary result set size: %lld\n", (long long)tcmaprnum(nmap));
  return nmap;
}


/* Check matching of one condition and a record.
   `qry' specifies the query object.
   `cond' specifies a condition object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   If they matches, the return value is true, else it is false. */
static bool tctdbqryonecondmatch(TDBQRY *qry, TDBCOND *cond, const char *pkbuf, int pksiz){
  assert(qry && cond && pkbuf && pksiz >= 0);
  if(cond->nsiz < 1)
    return tctdbqrycondmatch(cond->op, cond->expr, cond->esiz, pkbuf, pksiz) == cond->sign;
  int csiz;
  char *cbuf = tchdbget(qry->tdb->hdb, pkbuf, pksiz, &csiz);
  if(!cbuf) return false;
  bool rv;
  int vsiz;
  char *vbuf = tcmaploadone(cbuf, csiz, cond->name, cond->nsiz, &vsiz);
  if(vbuf){
    rv = tctdbqrycondmatch(cond->op, cond->expr, cond->esiz, vbuf, vsiz) == cond->sign;
    TCFREE(vbuf);
  } else {
    rv = !cond->sign;
  }
  TCFREE(cbuf);
  return rv;
}


/* Check matching of all conditions and a record.
   `qry' specifies the query object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   If they matches, the return value is true, else it is false. */
static bool tctdbqryallcondmatch(TDBQRY *qry, const char *pkbuf, int pksiz){
  assert(qry && pkbuf && pksiz >= 0);
  TCTDB *tdb = qry->tdb;
  TDBCOND *conds = qry->conds;
  int cnum = qry->cnum;
  int csiz;
  char *cbuf = tchdbget(tdb->hdb, pkbuf, pksiz, &csiz);
  if(!cbuf) return false;
  TCMAP *cols = tcmapload(cbuf, csiz);
  bool ok = true;
  for(int i = 0; i < cnum; i++){
    TDBCOND *cond = conds + i;
    if(!cond->alive) continue;
    if(cond->nsiz < 1){
      if(tctdbqrycondmatch(cond->op, cond->expr, cond->esiz, pkbuf, pksiz) != cond->sign){
        ok = false;
        break;
      }
    } else {
      int vsiz;
      const char *vbuf = tcmapget(cols, cond->name, cond->nsiz, &vsiz);
      if(vbuf){
        if(tctdbqrycondmatch(cond->op, cond->expr, cond->esiz, vbuf, vsiz) != cond->sign){
          ok = false;
          break;
        }
      } else {
        if(cond->sign){
          ok = false;
          break;
        }
      }
    }
  }
  tcmapdel(cols);
  TCFREE(cbuf);
  return ok;
}


/* Check matching of a operand expression and a column value.
   `op' specifies the operation type.
   `expr' specifies the operand expression.
   `esiz' specifies the size of the operand expression.
   `vbuf' specifies the column value.
   `vsiz' specifies the size of the column value.
   If they matches, the return value is true, else it is false. */
static bool tctdbqrycondmatch(int op, const char *expr, int esiz, const char *vbuf, int vsiz){
  assert(expr && esiz >= 0 && vbuf && vsiz >= 0);
  bool hit = false;
  switch(op){
  case TDBQCSTREQ:
    hit = vsiz == esiz && !memcmp(vbuf, expr, esiz);
    break;
  case TDBQCSTRINC:
    hit = strstr(vbuf, expr) != NULL;
    break;
  case TDBQCSTRBW:
    hit = tcstrfwm(vbuf, expr);
    break;
  case TDBQCSTREW:
    hit = tcstrbwm(vbuf, expr);
    break;
  case TDBQCSTRAND:
    hit = tctdbqrycondcheckstrand(vbuf, expr);
    break;
  case TDBQCSTROR:
    hit = tctdbqrycondcheckstror(vbuf, expr);
    break;
  case TDBQCSTROREQ:
    hit = tctdbqrycondcheckstroreq(vbuf, expr);
    break;
  case TDBQCSTRRX:
    hit = tcregexmatch(vbuf, expr);
    break;
  case TDBQCNUMEQ:
    hit = tcatoi(vbuf) == tcatoi(expr);
    break;
  case TDBQCNUMGT:
    hit = tcatoi(vbuf) > tcatoi(expr);
    break;
  case TDBQCNUMGE:
    hit = tcatoi(vbuf) >= tcatoi(expr);
    break;
  case TDBQCNUMLT:
    hit = tcatoi(vbuf) < tcatoi(expr);
    break;
  case TDBQCNUMLE:
    hit = tcatoi(vbuf) <= tcatoi(expr);
    break;
  case TDBQCNUMBT:
    hit = tctdbqrycondchecknumbt(vbuf, expr);
    break;
  case TDBQCNUMOREQ:
    hit = tctdbqrycondchecknumoreq(vbuf, expr);
    break;
  }
  return hit;
}


/* Check whether a string includes all tokens in another string.
   `vbuf' specifies the column value.
   `expr' specifies the operand expression.
   If they matches, the return value is true, else it is false. */
static bool tctdbqrycondcheckstrand(const char *vbuf, const char *expr){
  assert(vbuf && expr);
  const unsigned char *sp = (unsigned char *)expr;
  while(*sp != '\0'){
    while((*sp != '\0' && *sp <= ' ') || *sp == ','){
      sp++;
    }
    const unsigned char *ep = sp;
    while(*ep > ' ' && *ep != ','){
      ep++;
    }
    if(ep > sp){
      bool hit = false;
      const unsigned char *rp = (unsigned char *)vbuf;
      while(*rp != '\0'){
        const unsigned char *pp;
        for(pp = sp; pp < ep; pp++, rp++){
          if(*pp != *rp) break;
        }
        if(pp == ep && (*rp <= ' ' || *rp == ',')){
          hit = true;
          break;
        }
        while(*rp > ' ' && *rp != ','){
          rp++;
        }
        while((*rp != '\0' && *rp <= ' ') || *rp == ','){
          rp++;
        }
      }
      if(!hit) return false;
    }
    sp = ep;
  }
  return true;
}


/* Check whether a string includes at least one token in another string.
   `vbuf' specifies the target value.
   `expr' specifies the operation value.
   If they matches, the return value is true, else it is false. */
static bool tctdbqrycondcheckstror(const char *vbuf, const char *expr){
  assert(vbuf && expr);
  const unsigned char *sp = (unsigned char *)expr;
  while(*sp != '\0'){
    while((*sp != '\0' && *sp <= ' ') || *sp == ','){
      sp++;
    }
    const unsigned char *ep = sp;
    while(*ep > ' ' && *ep != ','){
      ep++;
    }
    if(ep > sp){
      bool hit = false;
      const unsigned char *rp = (unsigned char *)vbuf;
      while(*rp != '\0'){
        const unsigned char *pp;
        for(pp = sp; pp < ep; pp++, rp++){
          if(*pp != *rp) break;
        }
        if(pp == ep && (*rp <= ' ' || *rp == ',')){
          hit = true;
          break;
        }
        while(*rp > ' ' && *rp != ','){
          rp++;
        }
        while((*rp != '\0' && *rp <= ' ') || *rp == ','){
          rp++;
        }
      }
      if(hit) return true;
    }
    sp = ep;
  }
  return false;
}


/* Check whether a string is equal to at least one token in another string.
   `vbuf' specifies the target value.
   `expr' specifies the operation value.
   If they matches, the return value is true, else it is false. */
static bool tctdbqrycondcheckstroreq(const char *vbuf, const char *expr){
  assert(vbuf && expr);
  const unsigned char *sp = (unsigned char *)expr;
  while(*sp != '\0'){
    while((*sp != '\0' && *sp <= ' ') || *sp == ','){
      sp++;
    }
    const unsigned char *ep = sp;
    while(*ep > ' ' && *ep != ','){
      ep++;
    }
    if(ep > sp){
      const unsigned char *rp;
      for(rp = (unsigned char *)vbuf; *rp != '\0'; rp++){
        if(*sp != *rp || sp >= ep) break;
        sp++;
      }
      if(*rp == '\0' && sp == ep) return true;
    }
    sp = ep;
  }
  return false;
}


/* Check whether a decimal string is between two tokens in another string.
   `vbuf' specifies the target value.
   `expr' specifies the operation value.
   If they matches, the return value is true, else it is false. */
static bool tctdbqrycondchecknumbt(const char *vbuf, const char *expr){
  assert(vbuf && expr);
  while(*expr == ' ' || *expr == ','){
    expr++;
  }
  const char *pv = expr;
  while(*pv != '\0' && *pv != ' ' && *pv != ','){
    pv++;
  }
  if(*pv != ' ' && *pv != ',') pv = " ";
  pv++;
  while(*pv == ' ' || *pv == ','){
    pv++;
  }
  int64_t val = tcatoi(vbuf);
  int64_t lower = tcatoi(expr);
  int64_t upper = tcatoi(pv);
  if(lower > upper){
    int64_t swap = lower;
    lower = upper;
    upper = swap;
  }
  return val >= lower && val <= upper;
}


/* Check whether a number is equal to at least one token in another string.
   `vbuf' specifies the target value.
   `expr' specifies the operation value.
   If they matches, the return value is true, else it is false. */
static bool tctdbqrycondchecknumoreq(const char *vbuf, const char *expr){
  assert(vbuf && expr);
  int64_t vnum = tcatoi(vbuf);
  const char *sp = expr;
  while(*sp != '\0'){
    while(*sp == ' ' || *sp == ','){
      sp++;
    }
    const char *ep = sp;
    while(*ep != '\0' && *ep != ' ' && *ep != ','){
      ep++;
    }
    if(ep > sp && vnum == tcatoi(sp)) return true;
    sp = ep;
  }
  return false;
}


/* Compare two primary keys by number ascending.
   `a' specifies a key.
   `b' specifies of the other key.
   The return value is positive if the former is big, negative if the latter is big, 0 if both
   are equivalent. */
static int tdbcmppkeynumasc(const TCLISTDATUM *a, const TCLISTDATUM *b){
  return tccmpdecimal(a->ptr, a->size, b->ptr, b->size, NULL);
}


/* Compare two primary keys by number descending.
   `a' specifies a key.
   `b' specifies of the other key.
   The return value is positive if the former is big, negative if the latter is big, 0 if both
   are equivalent. */
static int tdbcmppkeynumdesc(const TCLISTDATUM *a, const TCLISTDATUM *b){
  return tccmpdecimal(b->ptr, b->size, a->ptr, a->size, NULL);
}


/* Compare two sort keys by string ascending.
   `a' specifies a key.
   `b' specifies of the other key.
   The return value is positive if the former is big, negative if the latter is big, 0 if both
   are equivalent. */
static int tdbcmpsortkeystrasc(const TDBSORTKEY *a, const TDBSORTKEY *b){
  if(!a->vbuf){
    if(!b->vbuf) return 0;
    return 1;
  }
  if(!b->vbuf){
    if(!a->vbuf) return 0;
    return -1;
  }
  int rv;
  TCCMPLEXICAL(rv, a->vbuf, a->vsiz, b->vbuf, b->vsiz);
  return rv;
}


/* Compare two sort keys by string descending.
   `a' specifies a key.
   `b' specifies of the other key.
   The return value is positive if the former is big, negative if the latter is big, 0 if both
   are equivalent. */
static int tdbcmpsortkeystrdesc(const TDBSORTKEY *a, const TDBSORTKEY *b){
  if(!a->vbuf){
    if(!b->vbuf) return 0;
    return 1;
  }
  if(!b->vbuf){
    if(!a->vbuf) return 0;
    return -1;
  }
  int rv;
  TCCMPLEXICAL(rv, a->vbuf, a->vsiz, b->vbuf, b->vsiz);
  return -rv;
}


/* Compare two sort keys by number ascending.
   `a' specifies a key.
   `b' specifies of the other key.
   The return value is positive if the former is big, negative if the latter is big, 0 if both
   are equivalent. */
static int tdbcmpsortkeynumasc(const TDBSORTKEY *a, const TDBSORTKEY *b){
  if(!a->vbuf){
    if(!b->vbuf) return 0;
    return 1;
  }
  if(!b->vbuf){
    if(!a->vbuf) return 0;
    return -1;
  }
  int64_t anum = tcatoi(a->vbuf);
  int64_t bnum = tcatoi(b->vbuf);
  if(anum < bnum) return -1;
  if(anum > bnum) return 1;
  return 0;
}


/* Compare two sort keys by number descending.
   `a' specifies a key.
   `b' specifies of the other key.
   The return value is positive if the former is big, negative if the latter is big, 0 if both
   are equivalent. */
static int tdbcmpsortkeynumdesc(const TDBSORTKEY *a, const TDBSORTKEY *b){
  if(!a->vbuf){
    if(!b->vbuf) return 0;
    return 1;
  }
  if(!b->vbuf){
    if(!a->vbuf) return 0;
    return -1;
  }
  int64_t anum = tcatoi(a->vbuf);
  int64_t bnum = tcatoi(b->vbuf);
  if(anum < bnum) return 1;
  if(anum > bnum) return -1;
  return 0;
}


/* Get the hash value of a record.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   The return value is the hash value. */
static uint16_t tctdbidxhash(const char *pkbuf, int pksiz){
  assert(pkbuf && pksiz && pksiz >= 0);
  uint32_t hash = 19780211;
  while(pksiz--){
    hash = hash * 37 + *(uint8_t *)pkbuf++;
  }
  return hash;
}


/* Add a record into indices of a table database object.
   `tdb' specifies the table database object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   `cols' specifies a map object containing columns.
   If successful, the return value is true, else, it is false. */
static bool tctdbidxput(TCTDB *tdb, const void *pkbuf, int pksiz, TCMAP *cols){
  assert(tdb && pkbuf && pksiz >= 0 && cols);
  bool err = false;
  uint16_t hash = tctdbidxhash(pkbuf, pksiz);
  TDBIDX *idxs = tdb->idxs;
  int inum = tdb->inum;
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    if(*(idx->name) != '\0') continue;
    switch(idx->type){
    case TDBITLEXICAL:
    case TDBITDECIMAL:
      if(!tcbdbput(idx->db, pkbuf, pksiz, pkbuf, pksiz)){
        tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
        err = true;
      }
      break;
    case TDBITTOKEN:
      if(!tctdbidxputtoken(tdb, idx, pkbuf, pksiz, pkbuf, pksiz)) err = true;
      break;
    }
  }
  tcmapiterinit(cols);
  const char *kbuf;
  int ksiz;
  while((kbuf = tcmapiternext(cols, &ksiz)) != NULL){
    int vsiz;
    const char *vbuf = tcmapiterval(kbuf, &vsiz);
    for(int i = 0; i < inum; i++){
      TDBIDX *idx = idxs + i;
      if(strcmp(idx->name, kbuf)) continue;
      switch(idx->type){
      case TDBITLEXICAL:
      case TDBITDECIMAL:
        if(!tctdbidxputone(tdb, idx, pkbuf, pksiz, hash, vbuf, vsiz)) err = true;
        break;
      case TDBITTOKEN:
        if(!tctdbidxputtoken(tdb, idx, pkbuf, pksiz, vbuf, vsiz)) err = true;
        break;
      }
    }
  }
  return !err;
}


/* Add a column of a record into an index of a table database object.
   `tdb' specifies the table database object.
   `idx' specifies the index object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   `hash' specifies the hash value of the primary key.
   `vbuf' specifies the pointer to the region of the column value.
   `vsiz' specifies the size of the region of the column value.
   If successful, the return value is true, else, it is false. */
static bool tctdbidxputone(TCTDB *tdb, TDBIDX *idx, const char *pkbuf, int pksiz, uint16_t hash,
                           const char *vbuf, int vsiz){
  assert(tdb && pkbuf && pksiz >= 0 && vbuf && vsiz);
  bool err = false;
  char stack[TDBCOLBUFSIZ], *rbuf;
  int rsiz = vsiz + 3;
  if(rsiz <= sizeof(stack)){
    rbuf = stack;
  } else {
    TCMALLOC(rbuf, rsiz);
  }
  memcpy(rbuf, vbuf, vsiz);
  rbuf[vsiz] = '\0';
  rbuf[vsiz+1] = hash >> 8;
  rbuf[vsiz+2] = hash & 0xff;
  if(!tcbdbputdup(idx->db, rbuf, rsiz, pkbuf, pksiz)){
    tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
    err = true;
  }
  if(rbuf != stack) TCFREE(rbuf);
  return !err;
}


/* Add a column of a record into an token inverted index of a table database object.
   `tdb' specifies the table database object.
   `idx' specifies the index object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   `vbuf' specifies the pointer to the region of the column value.
   `vsiz' specifies the size of the region of the column value.
   If successful, the return value is true, else, it is false. */
static bool tctdbidxputtoken(TCTDB *tdb, TDBIDX *idx, const char *pkbuf, int pksiz,
                             const char *vbuf, int vsiz){
  assert(tdb && idx && pkbuf && pksiz >= 0 && vbuf && vsiz >= 0);
  bool err = false;
  TCMAP *cc = idx->cc;
  uint64_t pkid = 0;
  for(int i = 0; i < pksiz; i++){
    int c = pkbuf[i];
    if(c >= '0' && c <= '9'){
      pkid = pkid * 10 + c - '0';
    } else {
      pkid = 0;
      break;
    }
  }
  char stack[TDBCOLBUFSIZ], *rbuf;
  int rsiz = pksiz + TCNUMBUFSIZ;
  if(rsiz < sizeof(stack)){
    rbuf = stack;
  } else {
    TCMALLOC(rbuf, rsiz);
  }
  const unsigned char *sp = (unsigned char *)vbuf;
  while(*sp != '\0'){
    while((*sp != '\0' && *sp <= ' ') || *sp == ','){
      sp++;
    }
    const unsigned char *ep = sp;
    while(*ep > ' ' && *ep != ','){
      ep++;
    }
    if(ep > sp){
      if(pkid > 0){
        TCSETVNUMBUF64(rsiz, rbuf, pkid);
        tcmapputcat(cc, sp, ep - sp, rbuf, rsiz);
      } else {
        char *wp = rbuf;
        *(wp++) = '\0';
        TCSETVNUMBUF(rsiz, wp, pksiz);
        wp += rsiz;
        memcpy(wp, pkbuf, pksiz);
        wp += pksiz;
        tcmapputcat(cc, sp, ep - sp, rbuf, wp - rbuf);
      }
    }
    sp = ep;
  }
  if(rbuf != stack) TCFREE(rbuf);
  if(tcmapmsiz(cc) > TDBIDXCCMAX && tctdbidxsynctoken(tdb, idx)) err = true;
  return !err;
}


/* Remove a record from indices of a table database object.
   `tdb' specifies the table database object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   `cols' specifies a map object containing columns.
   If successful, the return value is true, else, it is false. */
static bool tctdbidxout(TCTDB *tdb, const void *pkbuf, int pksiz, TCMAP *cols){
  assert(tdb && pkbuf && pksiz >= 0 && cols);
  bool err = false;
  uint16_t hash = tctdbidxhash(pkbuf, pksiz);
  TDBIDX *idxs = tdb->idxs;
  int inum = tdb->inum;
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    if(*(idx->name) != '\0') continue;
    switch(idx->type){
    case TDBITLEXICAL:
    case TDBITDECIMAL:
      if(!tcbdbout(idx->db, pkbuf, pksiz)){
        tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
        err = true;
      }
      break;
    case TDBITTOKEN:
      if(!tctdbidxouttoken(tdb, idx, pkbuf, pksiz, pkbuf, pksiz)) err = true;
      break;
    }
  }
  tcmapiterinit(cols);
  const char *kbuf;
  int ksiz;
  while((kbuf = tcmapiternext(cols, &ksiz)) != NULL){
    int vsiz;
    const char *vbuf = tcmapiterval(kbuf, &vsiz);
    for(int i = 0; i < inum; i++){
      TDBIDX *idx = idxs + i;
      if(strcmp(idx->name, kbuf)) continue;
      switch(idx->type){
      case TDBITLEXICAL:
      case TDBITDECIMAL:
        if(!tctdbidxoutone(tdb, idx, pkbuf, pksiz, hash, vbuf, vsiz)) err = true;
        break;
      case TDBITTOKEN:
        if(!tctdbidxouttoken(tdb, idx, pkbuf, pksiz, vbuf, vsiz)) err = true;
        break;
      }
    }
  }
  return !err;
}


/* Remove a column of a record from an index of a table database object.
   `tdb' specifies the table database object.
   `idx' specifies the index object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   `hash' specifies the hash value of the primary key.
   `vbuf' specifies the pointer to the region of the column value.
   `vsiz' specifies the size of the region of the column value.
   If successful, the return value is true, else, it is false. */
static bool tctdbidxoutone(TCTDB *tdb, TDBIDX *idx, const char *pkbuf, int pksiz, uint16_t hash,
                           const char *vbuf, int vsiz){
  assert(tdb && idx && pkbuf && pksiz >= 0 && vbuf && vsiz >= 0);
  bool err = false;
  char stack[TDBCOLBUFSIZ], *rbuf;
  int rsiz = vsiz + 3;
  if(rsiz <= sizeof(stack)){
    rbuf = stack;
  } else {
    TCMALLOC(rbuf, rsiz);
  }
  memcpy(rbuf, vbuf, vsiz);
  rbuf[vsiz] = '\0';
  rbuf[vsiz+1] = hash >> 8;
  rbuf[vsiz+2] = hash & 0xff;
  int ovsiz;
  const char *ovbuf = tcbdbget3(idx->db, rbuf, rsiz, &ovsiz);
  if(ovbuf && ovsiz == pksiz && !memcmp(ovbuf, pkbuf, ovsiz)){
    if(!tcbdbout(idx->db, rbuf, rsiz)){
      tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
      err = true;
    }
  } else {
    BDBCUR *cur = tcbdbcurnew(idx->db);
    if(tcbdbcurjump(cur, rbuf, rsiz)){
      int oksiz;
      const char *okbuf;
      while((okbuf = tcbdbcurkey3(cur, &oksiz)) != NULL){
        if(oksiz != rsiz || memcmp(okbuf, rbuf, oksiz)) break;
        ovbuf = tcbdbcurval3(cur, &ovsiz);
        if(ovsiz == pksiz && !memcmp(ovbuf, pkbuf, ovsiz)){
          if(!tcbdbcurout(cur)){
            tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
            err = true;
          }
          break;
        }
        tcbdbcurnext(cur);
      }
    } else {
      tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
      err = true;
    }
    tcbdbcurdel(cur);
  }
  if(rbuf != stack) TCFREE(rbuf);
  return !err;
}


/* Remove a column of a record from a token inverted index of a table database object.
   `tdb' specifies the table database object.
   `idx' specifies the index object.
   `pkbuf' specifies the pointer to the region of the primary key.
   `pksiz' specifies the size of the region of the primary key.
   `vbuf' specifies the pointer to the region of the column value.
   `vsiz' specifies the size of the region of the column value.
   If successful, the return value is true, else, it is false. */
static bool tctdbidxouttoken(TCTDB *tdb, TDBIDX *idx, const char *pkbuf, int pksiz,
                             const char *vbuf, int vsiz){
  assert(tdb && idx && pkbuf && pksiz >= 0 && vbuf && vsiz >= 0);
  bool err = false;
  TCBDB *db = idx->db;
  TCMAP *cc = idx->cc;
  uint64_t pkid = 0;
  for(int i = 0; i < pksiz; i++){
    int c = pkbuf[i];
    if(c >= '0' && c <= '9'){
      pkid = pkid * 10 + c - '0';
    } else {
      pkid = 0;
      break;
    }
  }
  TCXSTR *xstr = tcxstrnew();
  const unsigned char *sp = (unsigned char *)vbuf;
  while(*sp != '\0'){
    while((*sp != '\0' && *sp <= ' ') || *sp == ','){
      sp++;
    }
    const unsigned char *ep = sp;
    while(*ep > ' ' && *ep != ','){
      ep++;
    }
    if(ep > sp){
      tcxstrclear(xstr);
      int len = ep - sp;
      int csiz;
      const char *cbuf = tcmapget(cc, sp, len, &csiz);
      if(cbuf){
        while(csiz > 0){
          const char *pv = cbuf;
          if(*cbuf == '\0'){
            cbuf++;
            csiz--;
            int tsiz, step;
            TCREADVNUMBUF(cbuf, tsiz, step);
            cbuf += step;
            csiz -= step;
            if(tsiz != pksiz || memcmp(cbuf, pkbuf, tsiz)) TCXSTRCAT(xstr, pv, 1 + step + tsiz);
            cbuf += tsiz;
            csiz -= tsiz;
          } else {
            int64_t tid;
            int step;
            TCREADVNUMBUF64(cbuf, tid, step);
            if(tid != pkid) TCXSTRCAT(xstr, pv, step);
            cbuf += step;
            csiz -= step;
          }
        }
      }
      cbuf = tcbdbget3(db, sp, len, &csiz);
      if(cbuf){
        while(csiz > 0){
          const char *pv = cbuf;
          if(*cbuf == '\0'){
            cbuf++;
            csiz--;
            int tsiz, step;
            TCREADVNUMBUF(cbuf, tsiz, step);
            cbuf += step;
            csiz -= step;
            if(tsiz != pksiz || memcmp(cbuf, pkbuf, tsiz)) TCXSTRCAT(xstr, pv, 1 + step + tsiz);
            cbuf += tsiz;
            csiz -= tsiz;
          } else {
            int64_t tid;
            int step;
            TCREADVNUMBUF64(cbuf, tid, step);
            if(tid != pkid) TCXSTRCAT(xstr, pv, step);
            cbuf += step;
            csiz -= step;
          }
        }
        if(!tcbdbout(db, sp, len)){
          tctdbsetecode(tdb, tcbdbecode(db), __FILE__, __LINE__, __func__);
          err = true;
        }
      }
      tcmapput(cc, sp, len, TCXSTRPTR(xstr), TCXSTRSIZE(xstr));
    }
    sp = ep;
  }
  tcxstrdel(xstr);
  if(tcmapmsiz(cc) > TDBIDXCCMAX && tctdbidxsynctoken(tdb, idx)) err = true;
  return !err;
}


/* Synchronize updated contents of a token inverted index of a table database object.
   `tdb' specifies the table database object.
   `idx' specifies the index object.
   If successful, the return value is true, else, it is false. */
static bool tctdbidxsynctoken(TCTDB *tdb, TDBIDX *idx){
  assert(tdb && idx);
  TCBDB *db = idx->db;
  TCMAP *cc = idx->cc;
  bool err = false;
  int kn;
  const char **keys = tcmapkeys2(cc, &kn);
  for(int i = 0; i < kn; i++){
    const char *kbuf = keys[i];
    int ksiz = strlen(kbuf);
    int vsiz;
    const char *vbuf = tcmapget(cc, kbuf, ksiz, &vsiz);
    if(!tcbdbputcat(db, kbuf, ksiz, vbuf, vsiz)){
      tctdbsetecode(tdb, tcbdbecode(db), __FILE__, __LINE__, __func__);
      err = true;
    }
  }
  TCFREE(keys);
  tcmapclear(cc);
  return !err;
}


/* Retrieve records by a token inverted index of a table database object.
   `tdb' specifies the table database object.
   `idx' specifies the index object.
   `token' specifies the list object of tokens.
   `op' specifies the operation type.
   The return value is a map object of the primary keys of the corresponding records. */
static TCMAP *tctdbidxgetbytokens(TCTDB *tdb, TDBIDX *idx, const TCLIST *tokens, int op){
  assert(tdb && idx && tokens);
  TCBDB *db = idx->db;
  TCMAP *cc = idx->cc;
  int tnum = TCLISTNUM(tokens);
  TCMAP *res = tcmapnew();
  int cnt = 0;
  for(int i = 0; i < tnum; i++){
    const char *token;
    int tsiz;
    TCLISTVAL(token, tokens, i, tsiz);
    if(tsiz < 1) continue;
    TCMAP *wring = (cnt > 0 && op == TDBQCSTRAND) ? tcmapnew() : NULL;
    int csiz;
    const char *cbuf = tcmapget(cc, token, tsiz, &csiz);
    if(cbuf){
      while(csiz > 0){
        if(*cbuf == '\0'){
          cbuf++;
          csiz--;
          int tsiz, step;
          TCREADVNUMBUF(cbuf, tsiz, step);
          cbuf += step;
          csiz -= step;
          if(cnt < 1){
            tcmapput(res, cbuf, tsiz, "", 0);
          } else if(wring){
            int rsiz;
            if(tcmapget(res, cbuf, tsiz, &rsiz)) tcmapput(wring, cbuf, tsiz, "", 0);
          } else {
            tcmapput(res, cbuf, tsiz, "", 0);
          }
          cbuf += tsiz;
          csiz -= tsiz;
        } else {
          int64_t tid;
          int step;
          TCREADVNUMBUF64(cbuf, tid, step);
          char pkbuf[TCNUMBUFSIZ];
          int pksiz = sprintf(pkbuf, "%lld", (long long)tid);
          if(cnt < 1){
            tcmapput(res, pkbuf, pksiz, "", 0);
          } else if(wring){
            int rsiz;
            if(tcmapget(res, pkbuf, pksiz, &rsiz)) tcmapput(wring, pkbuf, pksiz, "", 0);
          } else {
            tcmapput(res, pkbuf, pksiz, "", 0);
          }
          cbuf += step;
          csiz -= step;
        }
      }
    }
    cbuf = tcbdbget3(db, token, tsiz, &csiz);
    if(cbuf){
      while(csiz > 0){
        if(*cbuf == '\0'){
          cbuf++;
          csiz--;
          int tsiz, step;
          TCREADVNUMBUF(cbuf, tsiz, step);
          cbuf += step;
          csiz -= step;
          if(cnt < 1){
            tcmapput(res, cbuf, tsiz, "", 0);
          } else if(wring){
            int rsiz;
            if(tcmapget(res, cbuf, tsiz, &rsiz)) tcmapput(wring, cbuf, tsiz, "", 0);
          } else {
            tcmapput(res, cbuf, tsiz, "", 0);
          }
          cbuf += tsiz;
          csiz -= tsiz;
        } else {
          int64_t tid;
          int step;
          TCREADVNUMBUF64(cbuf, tid, step);
          char pkbuf[TCNUMBUFSIZ];
          int pksiz = sprintf(pkbuf, "%lld", (long long)tid);
          if(cnt < 1){
            tcmapput(res, pkbuf, pksiz, "", 0);
          } else if(wring){
            int rsiz;
            if(tcmapget(res, pkbuf, pksiz, &rsiz)) tcmapput(wring, pkbuf, pksiz, "", 0);
          } else {
            tcmapput(res, pkbuf, pksiz, "", 0);
          }
          cbuf += step;
          csiz -= step;
        }
      }
    }
    if(wring){
      tcmapdel(res);
      res = wring;
    }
    cnt++;
  }
  return res;
}


/* Perform dynamic defragmentation of a table database object.
   `tdb' specifies the table database object.
   `step' specifie the number of steps.
   If successful, the return value is true, else, it is false. */
static bool tctdbdefragimpl(TCTDB *tdb, int64_t step){
  bool err = false;
  TCHDB *hdb = tdb->hdb;
  TDBIDX *idxs = tdb->idxs;
  int inum = tdb->inum;
  if(!tchdbdefrag(hdb, step)) err = true;
  for(int i = 0; i < inum; i++){
    TDBIDX *idx = idxs + i;
    switch(idx->type){
    case TDBITLEXICAL:
    case TDBITDECIMAL:
    case TDBITTOKEN:
      if(!tcbdbdefrag(idx->db, step)){
        tctdbsetecode(tdb, tcbdbecode(idx->db), __FILE__, __LINE__, __func__);
        err = true;
      }
      break;
    }
  }
  return !err;
}


/* Process each record atomically of a table database object.
   `tdb' specifies the table database object.
   `func' specifies the pointer to the iterator function called for each record.
   `op' specifies an arbitrary pointer to be given as a parameter of the iterator function.
   If successful, the return value is true, else, it is false. */
static bool tctdbforeachimpl(TCTDB *tdb, TCITER iter, void *op){
  assert(tdb && iter);
  TCHDB *hdb = tdb->hdb;
  char *lkbuf = NULL;
  int lksiz = 0;
  char *pkbuf, stack[TDBPAGEBUFSIZ], *rbuf;
  int pksiz;
  const char *cbuf;
  int csiz;
  while((pkbuf = tchdbgetnext3(hdb, lkbuf, lksiz, &pksiz, &cbuf, &csiz)) != NULL){
    if(pksiz < TDBPAGEBUFSIZ){
      rbuf = stack;
    } else {
      TCMALLOC(rbuf, pksiz + 1);
    }
    memcpy(rbuf, pkbuf, pksiz);
    stack[pksiz] = '\0';
    TCMAP *cols = tcmapload(cbuf, csiz);
    int zsiz;
    char *zbuf = tcstrjoin4(cols, &zsiz);
    bool rv = iter(rbuf, pksiz, zbuf, zsiz, op);
    TCFREE(zbuf);
    if(rbuf != stack) TCFREE(rbuf);
    tcmapdel(cols);
    TCFREE(lkbuf);
    lkbuf = pkbuf;
    lksiz = pksiz;
    if(!rv) break;
  }
  TCFREE(lkbuf);
  return true;
}


/* Answer to remove for each record of a query.
   `pkbuf' is ignored.
   `pksiz' is ignored.
   `op' is ignored.
   The return value is always `TDBQPOUT'. */
static int tctdbqryprocoutcb(const void *pkbuf, int pksiz, TCMAP *cols, void *op){
  assert(pkbuf && pksiz >= 0 && cols);
  return TDBQPOUT;
}


/* Lock a method of the table database object.
   `tdb' specifies the table database object.
   `wr' specifies whether the lock is writer or not.
   If successful, the return value is true, else, it is false. */
static bool tctdblockmethod(TCTDB *tdb, bool wr){
  assert(tdb);
  if(wr ? pthread_rwlock_wrlock(tdb->mmtx) != 0 : pthread_rwlock_rdlock(tdb->mmtx) != 0){
    tctdbsetecode(tdb, TCETHREAD, __FILE__, __LINE__, __func__);
    return false;
  }
  TCTESTYIELD();
  return true;
}


/* Unlock a method of the table database object.
   `tdb' specifies the table database object.
   If successful, the return value is true, else, it is false. */
static bool tctdbunlockmethod(TCTDB *tdb){
  assert(tdb);
  if(pthread_rwlock_unlock(tdb->mmtx) != 0){
    tctdbsetecode(tdb, TCETHREAD, __FILE__, __LINE__, __func__);
    return false;
  }
  TCTESTYIELD();
  return true;
}



/*************************************************************************************************
 * debugging functions
 *************************************************************************************************/


/* Print meta data of the header into the debugging output.
   `tdb' specifies the table database object. */
void tctdbprintmeta(TCTDB *tdb){
  assert(tdb);
  int dbgfd = tchdbdbgfd(tdb->hdb);
  if(dbgfd < 0) return;
  if(dbgfd == UINT16_MAX) dbgfd = 1;
  char buf[TDBPAGEBUFSIZ];
  char *wp = buf;
  wp += sprintf(wp, "META:");
  wp += sprintf(wp, " mmtx=%p", (void *)tdb->mmtx);
  wp += sprintf(wp, " hdb=%p", (void *)tdb->hdb);
  wp += sprintf(wp, " open=%d", tdb->open);
  wp += sprintf(wp, " wmode=%d", tdb->wmode);
  wp += sprintf(wp, " idxs=%p", (void *)tdb->idxs);
  wp += sprintf(wp, " inum=%d", tdb->inum);
  wp += sprintf(wp, " opts=%u", tdb->opts);
  wp += sprintf(wp, " lcnum=%d", tdb->lcnum);
  wp += sprintf(wp, " ncnum=%d", tdb->ncnum);
  wp += sprintf(wp, " tran=%d", tdb->tran);
  *(wp++) = '\n';
  tcwrite(dbgfd, buf, wp - buf);
}



// END OF FILE
