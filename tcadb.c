/*************************************************************************************************
 * The abstract database API of Tokyo Cabinet
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
#include "tcadb.h"
#include "myconf.h"



/*************************************************************************************************
 * API
 *************************************************************************************************/


/* Create an abstract database object. */
TCADB *tcadbnew(void){
  TCADB *adb;
  TCMALLOC(adb, sizeof(*adb));
  adb->name = NULL;
  adb->omode = ADBOVOID;
  adb->mdb = NULL;
  adb->ndb = NULL;
  adb->hdb = NULL;
  adb->bdb = NULL;
  adb->fdb = NULL;
  adb->capnum = -1;
  adb->capsiz = -1;
  adb->capcnt = 0;
  adb->cur = NULL;
  return adb;
}


/* Delete an abstract database object. */
void tcadbdel(TCADB *adb){
  assert(adb);
  if(adb->name) tcadbclose(adb);
  TCFREE(adb);
}


/* Open an abstract database. */
bool tcadbopen(TCADB *adb, const char *name){
  assert(adb && name);
  if(adb->name) return false;
  TCLIST *elems = tcstrsplit(name, "#");
  char *path = tclistshift2(elems);
  if(!path){
    tclistdel(elems);
    return false;
  }
  int64_t bnum = -1;
  int64_t capnum = -1;
  int64_t capsiz = -1;
  bool owmode = true;
  bool ocmode = true;
  bool otmode = false;
  bool onlmode = false;
  bool onbmode = false;
  int8_t apow = -1;
  int8_t fpow = -1;
  bool tlmode = false;
  bool tdmode = false;
  bool tbmode = false;
  bool ttmode = false;
  int32_t rcnum = -1;
  int64_t xmsiz = -1;
  int32_t lmemb = -1;
  int32_t nmemb = -1;
  int32_t lcnum = -1;
  int32_t ncnum = -1;
  int32_t width = -1;
  int64_t limsiz = -1;
  int ln = TCLISTNUM(elems);
  for(int i = 0; i < ln; i++){
    const char *elem = TCLISTVALPTR(elems, i);
    char *pv = strchr(elem, '=');
    if(!pv) continue;
    *(pv++) = '\0';
    if(!tcstricmp(elem, "bnum")){
      bnum = tcatoi(pv);
    } else if(!tcstricmp(elem, "capnum")){
      capnum = tcatoi(pv);
    } else if(!tcstricmp(elem, "capsiz")){
      capsiz = tcatoi(pv);
    } else if(!tcstricmp(elem, "mode")){
      owmode = strchr(pv, 'w') || strchr(pv, 'W');
      ocmode = strchr(pv, 'c') || strchr(pv, 'C');
      otmode = strchr(pv, 't') || strchr(pv, 'T');
      onlmode = strchr(pv, 'e') || strchr(pv, 'E');
      onbmode = strchr(pv, 'f') || strchr(pv, 'F');
    } else if(!tcstricmp(elem, "apow")){
      apow = tcatoi(pv);
    } else if(!tcstricmp(elem, "fpow")){
      fpow = tcatoi(pv);
    } else if(!tcstricmp(elem, "opts")){
      if(strchr(pv, 'l') || strchr(pv, 'L')) tlmode = true;
      if(strchr(pv, 'd') || strchr(pv, 'D')) tdmode = true;
      if(strchr(pv, 'b') || strchr(pv, 'B')) tbmode = true;
      if(strchr(pv, 't') || strchr(pv, 'T')) ttmode = true;
    } else if(!tcstricmp(elem, "rcnum")){
      rcnum = tcatoi(pv);
    } else if(!tcstricmp(elem, "xmsiz")){
      xmsiz = tcatoi(pv);
    } else if(!tcstricmp(elem, "lmemb")){
      lmemb = tcatoi(pv);
    } else if(!tcstricmp(elem, "nmemb")){
      nmemb = tcatoi(pv);
    } else if(!tcstricmp(elem, "lcnum")){
      lcnum = tcatoi(pv);
    } else if(!tcstricmp(elem, "ncnum")){
      ncnum = tcatoi(pv);
    } else if(!tcstricmp(elem, "width")){
      width = tcatoi(pv);
    } else if(!tcstricmp(elem, "limsiz")){
      limsiz = tcatoi(pv);
    }
  }
  tclistdel(elems);
  if(!tcstricmp(path, "*")){
    adb->mdb = bnum > 0 ? tcmdbnew2(bnum) : tcmdbnew();
    adb->capnum = capnum;
    adb->capsiz = capsiz;
    adb->capcnt = 0;
    adb->omode = ADBOMDB;
  } else if(!tcstricmp(path, "+")){
    adb->ndb = tcndbnew();
    adb->capnum = capnum;
    adb->capsiz = capsiz;
    adb->capcnt = 0;
    adb->omode = ADBONDB;
  } else if(tcstribwm(path, ".tch")){
    TCHDB *hdb = tchdbnew();
    tchdbsetmutex(hdb);
    int opts = 0;
    if(tlmode) opts |= HDBTLARGE;
    if(tdmode) opts |= HDBTDEFLATE;
    if(tbmode) opts |= HDBTBZIP;
    if(ttmode) opts |= HDBTTCBS;
    tchdbtune(hdb, bnum, apow, fpow, opts);
    tchdbsetcache(hdb, rcnum);
    if(xmsiz >= 0) tchdbsetxmsiz(hdb, xmsiz);
    int omode = owmode ? HDBOWRITER : HDBOREADER;
    if(ocmode) omode |= HDBOCREAT;
    if(otmode) omode |= HDBOTRUNC;
    if(onlmode) omode |= HDBONOLCK;
    if(onbmode) omode |= HDBOLCKNB;
    if(!tchdbopen(hdb, path, omode)){
      tchdbdel(hdb);
      TCFREE(path);
      return false;
    }
    adb->hdb = hdb;
    adb->omode = ADBOHDB;
  } else if(tcstribwm(path, ".tcb")){
    TCBDB *bdb = tcbdbnew();
    tcbdbsetmutex(bdb);
    int opts = 0;
    if(tlmode) opts |= BDBTLARGE;
    if(tdmode) opts |= BDBTDEFLATE;
    if(tbmode) opts |= BDBTBZIP;
    if(ttmode) opts |= BDBTTCBS;
    tcbdbtune(bdb, lmemb, nmemb, bnum, apow, fpow, opts);
    tcbdbsetcache(bdb, lcnum, ncnum);
    if(xmsiz >= 0) tcbdbsetxmsiz(bdb, xmsiz);
    if(capnum > 0) tcbdbsetcapnum(bdb, capnum);
    int omode = owmode ? BDBOWRITER : BDBOREADER;
    if(ocmode) omode |= BDBOCREAT;
    if(otmode) omode |= BDBOTRUNC;
    if(onlmode) omode |= BDBONOLCK;
    if(onbmode) omode |= BDBOLCKNB;
    if(!tcbdbopen(bdb, path, omode)){
      tcbdbdel(bdb);
      TCFREE(path);
      return false;
    }
    adb->bdb = bdb;
    adb->cur = tcbdbcurnew(bdb);
    adb->omode = ADBOBDB;
  } else if(tcstribwm(path, ".tcf")){
    TCFDB *fdb = tcfdbnew();
    tcfdbsetmutex(fdb);
    tcfdbtune(fdb, width, limsiz);
    int omode = owmode ? FDBOWRITER : FDBOREADER;
    if(ocmode) omode |= FDBOCREAT;
    if(otmode) omode |= FDBOTRUNC;
    if(onlmode) omode |= FDBONOLCK;
    if(onbmode) omode |= FDBOLCKNB;
    if(!tcfdbopen(fdb, path, omode)){
      tcfdbdel(fdb);
      TCFREE(path);
      return false;
    }
    adb->fdb = fdb;
    adb->omode = ADBOFDB;
  } else {
    TCFREE(path);
    return false;
  }
  TCFREE(path);
  adb->name = tcstrdup(name);
  return true;
}


/* Close an abstract database object. */
bool tcadbclose(TCADB *adb){
  assert(adb);
  int err = false;
  if(!adb->name) return false;
  switch(adb->omode){
  case ADBOMDB:
    tcmdbdel(adb->mdb);
    adb->mdb = NULL;
    break;
  case ADBONDB:
    tcndbdel(adb->ndb);
    adb->ndb = NULL;
    break;
  case ADBOHDB:
    if(!tchdbclose(adb->hdb)) err = true;
    tchdbdel(adb->hdb);
    adb->hdb = NULL;
    break;
  case ADBOBDB:
    tcbdbcurdel(adb->cur);
    if(!tcbdbclose(adb->bdb)) err = true;
    tcbdbdel(adb->bdb);
    adb->bdb = NULL;
    break;
  case ADBOFDB:
    if(!tcfdbclose(adb->fdb)) err = true;
    tcfdbdel(adb->fdb);
    adb->fdb = NULL;
    break;
  default:
    err = true;
    break;
  }
  TCFREE(adb->name);
  adb->name = NULL;
  adb->omode = ADBOVOID;
  return !err;
}


/* Store a record into an abstract database object. */
bool tcadbput(TCADB *adb, const void *kbuf, int ksiz, const void *vbuf, int vsiz){
  assert(adb && kbuf && ksiz >= 0 && vbuf && vsiz >= 0);
  bool err = false;
  switch(adb->omode){
  case ADBOMDB:
    if(adb->capnum > 0 || adb->capsiz > 0){
      tcmdbput3(adb->mdb, kbuf, ksiz, vbuf, vsiz);
      adb->capcnt++;
      if((adb->capcnt & 0xff) == 0){
        if(adb->capnum > 0 && tcmdbrnum(adb->mdb) > adb->capnum)
          tcmdbcutfront(adb->mdb, 0x100);
        if(adb->capsiz > 0 && tcmdbmsiz(adb->mdb) > adb->capsiz)
          tcmdbcutfront(adb->mdb, 0x200);
      }
    } else {
      tcmdbput(adb->mdb, kbuf, ksiz, vbuf, vsiz);
    }
    break;
  case ADBONDB:
    tcndbput(adb->ndb, kbuf, ksiz, vbuf, vsiz);
    if(adb->capnum > 0 || adb->capsiz > 0){
      adb->capcnt++;
      if((adb->capcnt & 0xff) == 0){
        if(adb->capnum > 0 && tcndbrnum(adb->ndb) > adb->capnum)
          tcndbcutfringe(adb->ndb, 0x100);
        if(adb->capsiz > 0 && tcndbmsiz(adb->ndb) > adb->capsiz)
          tcndbcutfringe(adb->ndb, 0x200);
      }
    }
    break;
  case ADBOHDB:
    if(!tchdbput(adb->hdb, kbuf, ksiz, vbuf, vsiz)) err = true;
    break;
  case ADBOBDB:
    if(!tcbdbput(adb->bdb, kbuf, ksiz, vbuf, vsiz)) err = true;
    break;
  case ADBOFDB:
    if(!tcfdbput2(adb->fdb, kbuf, ksiz, vbuf, vsiz)) err = true;
    break;
  default:
    err = true;
    break;
  }
  return !err;
}


/* Store a string record into an abstract object. */
bool tcadbput2(TCADB *adb, const char *kstr, const char *vstr){
  assert(adb && kstr && vstr);
  return tcadbput(adb, kstr, strlen(kstr), vstr, strlen(vstr));
}


/* Store a new record into an abstract database object. */
bool tcadbputkeep(TCADB *adb, const void *kbuf, int ksiz, const void *vbuf, int vsiz){
  assert(adb && kbuf && ksiz >= 0 && vbuf && vsiz >= 0);
  bool err = false;
  switch(adb->omode){
  case ADBOMDB:
    if(tcmdbputkeep(adb->mdb, kbuf, ksiz, vbuf, vsiz)){
      if(adb->capnum > 0 || adb->capsiz > 0){
        adb->capcnt++;
        if((adb->capcnt & 0xff) == 0){
          if(adb->capnum > 0 && tcmdbrnum(adb->mdb) > adb->capnum)
            tcmdbcutfront(adb->mdb, 0x100);
          if(adb->capsiz > 0 && tcmdbmsiz(adb->mdb) > adb->capsiz)
            tcmdbcutfront(adb->mdb, 0x200);
        }
      }
    } else {
      err = true;
    }
    break;
  case ADBONDB:
    if(tcndbputkeep(adb->ndb, kbuf, ksiz, vbuf, vsiz)){
      if(adb->capnum > 0 || adb->capsiz > 0){
        adb->capcnt++;
        if((adb->capcnt & 0xff) == 0){
          if(adb->capnum > 0 && tcndbrnum(adb->ndb) > adb->capnum)
            tcndbcutfringe(adb->ndb, 0x100);
          if(adb->capsiz > 0 && tcndbmsiz(adb->ndb) > adb->capsiz)
            tcndbcutfringe(adb->ndb, 0x200);
        }
      }
    } else {
      err = true;
    }
    break;
  case ADBOHDB:
    if(!tchdbputkeep(adb->hdb, kbuf, ksiz, vbuf, vsiz)) err = true;
    break;
  case ADBOBDB:
    if(!tcbdbputkeep(adb->bdb, kbuf, ksiz, vbuf, vsiz)) err = true;
    break;
  case ADBOFDB:
    if(!tcfdbputkeep2(adb->fdb, kbuf, ksiz, vbuf, vsiz)) err = true;
    break;
  default:
    err = true;
    break;
  }
  return !err;
}


/* Store a new string record into an abstract database object. */
bool tcadbputkeep2(TCADB *adb, const char *kstr, const char *vstr){
  assert(adb && kstr && vstr);
  return tcadbputkeep(adb, kstr, strlen(kstr), vstr, strlen(vstr));
}


/* Concatenate a value at the end of the existing record in an abstract database object. */
bool tcadbputcat(TCADB *adb, const void *kbuf, int ksiz, const void *vbuf, int vsiz){
  assert(adb && kbuf && ksiz >= 0 && vbuf && vsiz >= 0);
  bool err = false;
  switch(adb->omode){
  case ADBOMDB:
    if(adb->capnum > 0 || adb->capsiz > 0){
      tcmdbputcat3(adb->mdb, kbuf, ksiz, vbuf, vsiz);
      adb->capcnt++;
      if((adb->capcnt & 0xff) == 0){
        if(adb->capnum > 0 && tcmdbrnum(adb->mdb) > adb->capnum)
          tcmdbcutfront(adb->mdb, 0x100);
        if(adb->capsiz > 0 && tcmdbmsiz(adb->mdb) > adb->capsiz)
          tcmdbcutfront(adb->mdb, 0x200);
      }
    } else {
      tcmdbputcat(adb->mdb, kbuf, ksiz, vbuf, vsiz);
    }
    break;
  case ADBONDB:
    tcndbputcat(adb->ndb, kbuf, ksiz, vbuf, vsiz);
    if(adb->capnum > 0 || adb->capsiz > 0){
      adb->capcnt++;
      if((adb->capcnt & 0xff) == 0){
        if(adb->capnum > 0 && tcndbrnum(adb->ndb) > adb->capnum)
          tcndbcutfringe(adb->ndb, 0x100);
        if(adb->capsiz > 0 && tcndbmsiz(adb->ndb) > adb->capsiz)
          tcndbcutfringe(adb->ndb, 0x200);
      }
    }
    break;
  case ADBOHDB:
    if(!tchdbputcat(adb->hdb, kbuf, ksiz, vbuf, vsiz)) err = true;
    break;
  case ADBOBDB:
    if(!tcbdbputcat(adb->bdb, kbuf, ksiz, vbuf, vsiz)) err = true;
    break;
  case ADBOFDB:
    if(!tcfdbputcat2(adb->fdb, kbuf, ksiz, vbuf, vsiz)) err = true;
    break;
  default:
    err = true;
    break;
  }
  return !err;
}


/* Concatenate a string value at the end of the existing record in an abstract database object. */
bool tcadbputcat2(TCADB *adb, const char *kstr, const char *vstr){
  assert(adb && kstr && vstr);
  return tcadbputcat(adb, kstr, strlen(kstr), vstr, strlen(vstr));
}


/* Remove a record of an abstract database object. */
bool tcadbout(TCADB *adb, const void *kbuf, int ksiz){
  assert(adb && kbuf && ksiz >= 0);
  bool err = false;
  switch(adb->omode){
  case ADBOMDB:
    if(!tcmdbout(adb->mdb, kbuf, ksiz)) err = true;
    break;
  case ADBONDB:
    if(!tcndbout(adb->ndb, kbuf, ksiz)) err = true;
    break;
  case ADBOHDB:
    if(!tchdbout(adb->hdb, kbuf, ksiz)) err = true;
    break;
  case ADBOBDB:
    if(!tcbdbout(adb->bdb, kbuf, ksiz)) err = true;
    break;
  case ADBOFDB:
    if(!tcfdbout2(adb->fdb, kbuf, ksiz)) err = true;
    break;
  default:
    err = true;
    break;
  }
  return !err;
}


/* Remove a string record of an abstract database object. */
bool tcadbout2(TCADB *adb, const char *kstr){
  assert(adb && kstr);
  return tcadbout(adb, kstr, strlen(kstr));
}


/* Retrieve a record in an abstract database object. */
void *tcadbget(TCADB *adb, const void *kbuf, int ksiz, int *sp){
  assert(adb && kbuf && ksiz >= 0 && sp);
  char *rv;
  switch(adb->omode){
  case ADBOMDB:
    rv = tcmdbget(adb->mdb, kbuf, ksiz, sp);
    break;
  case ADBONDB:
    rv = tcndbget(adb->ndb, kbuf, ksiz, sp);
    break;
  case ADBOHDB:
    rv = tchdbget(adb->hdb, kbuf, ksiz, sp);
    break;
  case ADBOBDB:
    rv = tcbdbget(adb->bdb, kbuf, ksiz, sp);
    break;
  case ADBOFDB:
    rv = tcfdbget2(adb->fdb, kbuf, ksiz, sp);
    break;
  default:
    rv = NULL;
    break;
  }
  return rv;
}


/* Retrieve a string record in an abstract database object. */
char *tcadbget2(TCADB *adb, const char *kstr){
  assert(adb && kstr);
  int vsiz;
  return tcadbget(adb, kstr, strlen(kstr), &vsiz);
}


/* Get the size of the value of a record in an abstract database object. */
int tcadbvsiz(TCADB *adb, const void *kbuf, int ksiz){
  assert(adb && kbuf && ksiz >= 0);
  int rv;
  switch(adb->omode){
  case ADBOMDB:
    rv = tcmdbvsiz(adb->mdb, kbuf, ksiz);
    break;
  case ADBONDB:
    rv = tcndbvsiz(adb->ndb, kbuf, ksiz);
    break;
  case ADBOHDB:
    rv = tchdbvsiz(adb->hdb, kbuf, ksiz);
    break;
  case ADBOBDB:
    rv = tcbdbvsiz(adb->bdb, kbuf, ksiz);
    break;
  case ADBOFDB:
    rv = tcfdbvsiz2(adb->fdb, kbuf, ksiz);
    break;
  default:
    rv = -1;
    break;
  }
  return rv;
}


/* Get the size of the value of a string record in an abstract database object. */
int tcadbvsiz2(TCADB *adb, const char *kstr){
  assert(adb && kstr);
  return tcadbvsiz(adb, kstr, strlen(kstr));
}


/* Initialize the iterator of an abstract database object. */
bool tcadbiterinit(TCADB *adb){
  assert(adb);
  bool err = false;
  switch(adb->omode){
  case ADBOMDB:
    tcmdbiterinit(adb->mdb);
    break;
  case ADBONDB:
    tcndbiterinit(adb->ndb);
    break;
  case ADBOHDB:
    if(!tchdbiterinit(adb->hdb)) err = true;
    break;
  case ADBOBDB:
    if(!tcbdbcurfirst(adb->cur)){
      int ecode = tcbdbecode(adb->bdb);
      if(ecode != TCESUCCESS && ecode != TCEINVALID && ecode != TCEKEEP && ecode != TCENOREC)
        err = true;
    }
    break;
  case ADBOFDB:
    if(!tcfdbiterinit(adb->fdb)) err = true;
    break;
  default:
    err = true;
    break;
  }
  return !err;
}


/* Get the next key of the iterator of an abstract database object. */
void *tcadbiternext(TCADB *adb, int *sp){
  assert(adb && sp);
  char *rv;
  switch(adb->omode){
  case ADBOMDB:
    rv = tcmdbiternext(adb->mdb, sp);
    break;
  case ADBONDB:
    rv = tcndbiternext(adb->ndb, sp);
    break;
  case ADBOHDB:
    rv = tchdbiternext(adb->hdb, sp);
    break;
  case ADBOBDB:
    rv = tcbdbcurkey(adb->cur, sp);
    tcbdbcurnext(adb->cur);
    break;
  case ADBOFDB:
    rv = tcfdbiternext2(adb->fdb, sp);
    break;
  default:
    rv = NULL;
    break;
  }
  return rv;
}


/* Get the next key string of the iterator of an abstract database object. */
char *tcadbiternext2(TCADB *adb){
  assert(adb);
  int vsiz;
  return tcadbiternext(adb, &vsiz);
}


/* Get forward matching keys in an abstract database object. */
TCLIST *tcadbfwmkeys(TCADB *adb, const void *pbuf, int psiz, int max){
  assert(adb && pbuf && psiz >= 0);
  TCLIST *rv;
  switch(adb->omode){
  case ADBOMDB:
    rv = tcmdbfwmkeys(adb->mdb, pbuf, psiz, max);
    break;
  case ADBONDB:
    rv = tcndbfwmkeys(adb->ndb, pbuf, psiz, max);
    break;
  case ADBOHDB:
    rv = tchdbfwmkeys(adb->hdb, pbuf, psiz, max);
    break;
  case ADBOBDB:
    rv = tcbdbfwmkeys(adb->bdb, pbuf, psiz, max);
    break;
  case ADBOFDB:
    rv = tcfdbrange4(adb->fdb, pbuf, psiz, max);
    break;
  default:
    rv = tclistnew();
    break;
  }
  return rv;
}


/* Get forward matching string keys in an abstract database object. */
TCLIST *tcadbfwmkeys2(TCADB *adb, const char *pstr, int max){
  assert(adb && pstr);
  return tcadbfwmkeys(adb, pstr, strlen(pstr), max);
}


/* Add an integer to a record in an abstract database object. */
int tcadbaddint(TCADB *adb, const void *kbuf, int ksiz, int num){
  assert(adb && kbuf && ksiz >= 0);
  int rv;
  switch(adb->omode){
  case ADBOMDB:
    rv = tcmdbaddint(adb->mdb, kbuf, ksiz, num);
    if(adb->capnum > 0 || adb->capsiz > 0){
      adb->capcnt++;
      if((adb->capcnt & 0xff) == 0){
        if(adb->capnum > 0 && tcmdbrnum(adb->mdb) > adb->capnum)
          tcmdbcutfront(adb->mdb, 0x100);
        if(adb->capsiz > 0 && tcmdbmsiz(adb->mdb) > adb->capsiz)
          tcmdbcutfront(adb->mdb, 0x200);
      }
    }
    break;
  case ADBONDB:
    rv = tcndbaddint(adb->ndb, kbuf, ksiz, num);
    if(adb->capnum > 0 || adb->capsiz > 0){
      adb->capcnt++;
      if((adb->capcnt & 0xff) == 0){
        if(adb->capnum > 0 && tcndbrnum(adb->ndb) > adb->capnum)
          tcndbcutfringe(adb->ndb, 0x100);
        if(adb->capsiz > 0 && tcndbmsiz(adb->ndb) > adb->capsiz)
          tcndbcutfringe(adb->ndb, 0x200);
      }
    }
    break;
  case ADBOHDB:
    rv = tchdbaddint(adb->hdb, kbuf, ksiz, num);
    break;
  case ADBOBDB:
    rv = tcbdbaddint(adb->bdb, kbuf, ksiz, num);
    break;
  case ADBOFDB:
    rv = tcfdbaddint(adb->fdb, tcfdbkeytoid(kbuf, ksiz), num);
    break;
  default:
    rv = INT_MIN;
    break;
  }
  return rv;
}


/* Add a real number to a record in an abstract database object. */
double tcadbadddouble(TCADB *adb, const void *kbuf, int ksiz, double num){
  assert(adb && kbuf && ksiz >= 0);
  double rv;
  switch(adb->omode){
  case ADBOMDB:
    rv = tcmdbadddouble(adb->mdb, kbuf, ksiz, num);
    if(adb->capnum > 0 || adb->capsiz > 0){
      adb->capcnt++;
      if((adb->capcnt & 0xff) == 0){
        if(adb->capnum > 0 && tcmdbrnum(adb->mdb) > adb->capnum)
          tcmdbcutfront(adb->mdb, 0x100);
        if(adb->capsiz > 0 && tcmdbmsiz(adb->mdb) > adb->capsiz)
          tcmdbcutfront(adb->mdb, 0x200);
      }
    }
    break;
  case ADBONDB:
    rv = tcndbadddouble(adb->ndb, kbuf, ksiz, num);
    if(adb->capnum > 0 || adb->capsiz > 0){
      adb->capcnt++;
      if((adb->capcnt & 0xff) == 0){
        if(adb->capnum > 0 && tcndbrnum(adb->ndb) > adb->capnum)
          tcndbcutfringe(adb->ndb, 0x100);
        if(adb->capsiz > 0 && tcndbmsiz(adb->ndb) > adb->capsiz)
          tcndbcutfringe(adb->ndb, 0x200);
      }
    }
    break;
  case ADBOHDB:
    rv = tchdbadddouble(adb->hdb, kbuf, ksiz, num);
    break;
  case ADBOBDB:
    rv = tcbdbadddouble(adb->bdb, kbuf, ksiz, num);
    break;
  case ADBOFDB:
    rv = tcfdbadddouble(adb->fdb, tcfdbkeytoid(kbuf, ksiz), num);
    break;
  default:
    rv = nan("");
    break;
  }
  return rv;
}


/* Synchronize updated contents of an abstract database object with the file and the device. */
bool tcadbsync(TCADB *adb){
  assert(adb);
  bool err = false;
  switch(adb->omode){
  case ADBOMDB:
    if(adb->capnum > 0){
      while(tcmdbrnum(adb->mdb) > adb->capnum){
        tcmdbcutfront(adb->mdb, 1);
      }
    }
    if(adb->capsiz > 0){
      while(tcmdbmsiz(adb->mdb) > adb->capsiz && tcmdbrnum(adb->mdb) > 0){
        tcmdbcutfront(adb->mdb, 1);
      }
    }
    break;
  case ADBONDB:
    if(adb->capnum > 0 && tcndbrnum(adb->ndb) > adb->capnum)
      tcndbcutfringe(adb->ndb, tcndbrnum(adb->ndb) - adb->capnum);
    if(adb->capsiz > 0){
      while(tcndbmsiz(adb->ndb) > adb->capsiz && tcndbrnum(adb->ndb) > 0){
        tcndbcutfringe(adb->ndb, 0x100);
      }
    }
    break;
  case ADBOHDB:
    if(!tchdbsync(adb->hdb)) err = true;
    break;
  case ADBOBDB:
    if(!tcbdbsync(adb->bdb)) err = true;
    break;
  case ADBOFDB:
    if(!tcfdbsync(adb->fdb)) err = true;
    break;
  default:
    err = true;
    break;
  }
  return !err;
}


/* Remove all records of an abstract database object. */
bool tcadbvanish(TCADB *adb){
  assert(adb);
  bool err = false;
  switch(adb->omode){
  case ADBOMDB:
    tcmdbvanish(adb->mdb);
    break;
  case ADBONDB:
    tcndbvanish(adb->ndb);
    break;
  case ADBOHDB:
    if(!tchdbvanish(adb->hdb)) err = true;
    break;
  case ADBOBDB:
    if(!tcbdbvanish(adb->bdb)) err = true;
    break;
  case ADBOFDB:
    if(!tcfdbvanish(adb->fdb)) err = true;
    break;
  default:
    err = true;
    break;
  }
  return !err;
}


/* Copy the database file of an abstract database object. */
bool tcadbcopy(TCADB *adb, const char *path){
  assert(adb && path);
  bool err = false;
  switch(adb->omode){
  case ADBOMDB:
  case ADBONDB:
    if(*path == '@'){
      char tsbuf[TCNUMBUFSIZ];
      sprintf(tsbuf, "%llu", (unsigned long long)(tctime() * 1000000));
      const char *args[3];
      args[0] = path + 1;
      args[1] = adb->name;
      args[2] = tsbuf;
      if(tcsystem(args, sizeof(args) / sizeof(*args)) != 0) err = true;
    } else {
      TCADB *tadb = tcadbnew();
      if(tcadbopen(tadb, path)){
        tcadbiterinit(adb);
        char *kbuf;
        int ksiz;
        while((kbuf = tcadbiternext(adb, &ksiz)) != NULL){
          int vsiz;
          char *vbuf = tcadbget(adb, kbuf, ksiz, &vsiz);
          if(vbuf){
            if(!tcadbput(tadb, kbuf, ksiz, vbuf, vsiz)) err = true;
            TCFREE(vbuf);
          }
          TCFREE(kbuf);
        }
        if(!tcadbclose(tadb)) err = true;
      } else {
        err = true;
      }
      tcadbdel(tadb);
    }
    break;
  case ADBOHDB:
    if(!tchdbcopy(adb->hdb, path)) err = true;
    break;
  case ADBOBDB:
    if(!tcbdbcopy(adb->bdb, path)) err = true;
    break;
  case ADBOFDB:
    if(!tcfdbcopy(adb->fdb, path)) err = true;
    break;
  default:
    err = true;
    break;
  }
  return !err;
}


/* Get the number of records of an abstract database object. */
uint64_t tcadbrnum(TCADB *adb){
  assert(adb);
  uint64_t rv;
  switch(adb->omode){
  case ADBOMDB:
    rv = tcmdbrnum(adb->mdb);
    break;
  case ADBONDB:
    rv = tcndbrnum(adb->ndb);
    break;
  case ADBOHDB:
    rv = tchdbrnum(adb->hdb);
    break;
  case ADBOBDB:
    rv = tcbdbrnum(adb->bdb);
    break;
  case ADBOFDB:
    rv = tcfdbrnum(adb->fdb);
    break;
  default:
    rv = 0;
    break;
  }
  return rv;
}


/* Get the size of the database of an abstract database object. */
uint64_t tcadbsize(TCADB *adb){
  assert(adb);
  uint64_t rv;
  switch(adb->omode){
  case ADBOMDB:
    rv = tcmdbmsiz(adb->mdb);
    break;
  case ADBONDB:
    rv = tcndbmsiz(adb->ndb);
    break;
  case ADBOHDB:
    rv = tchdbfsiz(adb->hdb);
    break;
  case ADBOBDB:
    rv = tcbdbfsiz(adb->bdb);
    break;
  case ADBOFDB:
    rv = tcfdbfsiz(adb->fdb);
    break;
  default:
    rv = 0;
    break;
  }
  return rv;
}


/* Call a versatile function for miscellaneous operations of an abstract database object. */
TCLIST *tcadbmisc(TCADB *adb, const char *name, const TCLIST *args){
  assert(adb && name && args);
  int argc = tclistnum(args);
  TCLIST *rv;
  switch(adb->omode){
  case ADBOMDB:
    if(!strcmp(name, "putlist")){
      rv = tclistnew();
      argc--;
      for(int i = 0; i < argc; i += 2){
        int ksiz;
        const char *kbuf = tclistval(args, i, &ksiz);
        int vsiz;
        const char *vbuf = tclistval(args, i + 1, &vsiz);
        tcmdbput(adb->mdb, kbuf, ksiz, vbuf, vsiz);
      }
    } else if(!strcmp(name, "outlist")){
      rv = tclistnew();
      for(int i = 0; i < argc; i++){
        int ksiz;
        const char *kbuf = tclistval(args, i, &ksiz);
        tcmdbout(adb->mdb, kbuf, ksiz);
      }
    } else if(!strcmp(name, "getlist")){
      rv = tclistnew2(argc);
      for(int i = 0; i < argc; i++){
        int ksiz;
        const char *kbuf = tclistval(args, i, &ksiz);
        int vsiz;
        char *vbuf = tcmdbget(adb->mdb, kbuf, ksiz, &vsiz);
        if(vbuf){
          tclistpush(rv, kbuf, ksiz);
          tclistpush(rv, vbuf, vsiz);
          tcfree(vbuf);
        }
      }
    } else {
      rv = NULL;
    }
    break;
  case ADBONDB:
    if(!strcmp(name, "putlist")){
      rv = tclistnew();
      argc--;
      for(int i = 0; i < argc; i += 2){
        int ksiz;
        const char *kbuf = tclistval(args, i, &ksiz);
        int vsiz;
        const char *vbuf = tclistval(args, i + 1, &vsiz);
        tcndbput(adb->ndb, kbuf, ksiz, vbuf, vsiz);
      }
    } else if(!strcmp(name, "outlist")){
      rv = tclistnew();
      for(int i = 0; i < argc; i++){
        int ksiz;
        const char *kbuf = tclistval(args, i, &ksiz);
        tcndbout(adb->ndb, kbuf, ksiz);
      }
    } else if(!strcmp(name, "getlist")){
      rv = tclistnew2(argc);
      for(int i = 0; i < argc; i++){
        int ksiz;
        const char *kbuf = tclistval(args, i, &ksiz);
        int vsiz;
        char *vbuf = tcndbget(adb->ndb, kbuf, ksiz, &vsiz);
        if(vbuf){
          tclistpush(rv, kbuf, ksiz);
          tclistpush(rv, vbuf, vsiz);
          tcfree(vbuf);
        }
      }
    } else {
      rv = NULL;
    }
    break;
  case ADBOHDB:
    if(!strcmp(name, "putlist")){
      rv = tclistnew();
      bool err = false;
      argc--;
      for(int i = 0; i < argc; i += 2){
        int ksiz;
        const char *kbuf = tclistval(args, i, &ksiz);
        int vsiz;
        const char *vbuf = tclistval(args, i + 1, &vsiz);
        if(!tchdbput(adb->hdb, kbuf, ksiz, vbuf, vsiz)){
          err = true;
          break;
        }
      }
      if(err){
        tclistdel(rv);
        rv = NULL;
      }
    } else if(!strcmp(name, "outlist")){
      rv = tclistnew();
      bool err = false;
      for(int i = 0; i < argc; i++){
        int ksiz;
        const char *kbuf = tclistval(args, i, &ksiz);
        if(!tchdbout(adb->hdb, kbuf, ksiz) && tchdbecode(adb->hdb) != TCENOREC){
          err = true;
          break;
        }
      }
      if(err){
        tclistdel(rv);
        rv = NULL;
      }
    } else if(!strcmp(name, "getlist")){
      rv = tclistnew2(argc);
      bool err = false;
      for(int i = 0; i < argc; i++){
        int ksiz;
        const char *kbuf = tclistval(args, i, &ksiz);
        int vsiz;
        char *vbuf = tchdbget(adb->hdb, kbuf, ksiz, &vsiz);
        if(vbuf){
          tclistpush(rv, kbuf, ksiz);
          tclistpush(rv, vbuf, vsiz);
          tcfree(vbuf);
        } else if(tchdbecode(adb->hdb) != TCENOREC){
          err = true;
        }
      }
      if(err){
        tclistdel(rv);
        rv = NULL;
      }
    } else {
      rv = NULL;
    }
    break;
  case ADBOBDB:
    if(!strcmp(name, "putlist")){
      rv = tclistnew();
      bool err = false;
      argc--;
      for(int i = 0; i < argc; i += 2){
        int ksiz;
        const char *kbuf = tclistval(args, i, &ksiz);
        int vsiz;
        const char *vbuf = tclistval(args, i + 1, &vsiz);
        if(!tcbdbputdup(adb->bdb, kbuf, ksiz, vbuf, vsiz)){
          err = true;
          break;
        }
      }
      if(err){
        tclistdel(rv);
        rv = NULL;
      }
    } else if(!strcmp(name, "outlist")){
      rv = tclistnew();
      bool err = false;
      for(int i = 0; i < argc; i++){
        int ksiz;
        const char *kbuf = tclistval(args, i, &ksiz);
        if(!tcbdbout3(adb->bdb, kbuf, ksiz) && tcbdbecode(adb->bdb) != TCENOREC){
          err = true;
          break;
        }
      }
      if(err){
        tclistdel(rv);
        rv = NULL;
      }
    } else if(!strcmp(name, "getlist")){
      rv = tclistnew2(argc);
      bool err = false;
      for(int i = 0; i < argc; i++){
        int ksiz;
        const char *kbuf = tclistval(args, i, &ksiz);
        TCLIST *vals = tcbdbget4(adb->bdb, kbuf, ksiz);
        if(vals){
          int vnum = tclistnum(vals);
          for(int j = 0; j < vnum; j++){
            tclistpush(rv, kbuf, ksiz);
            int vsiz;
            const char *vbuf = tclistval(vals, j, &vsiz);
            tclistpush(rv, vbuf, vsiz);
          }
          tclistdel(vals);
        } else if(tcbdbecode(adb->bdb) != TCENOREC){
          err = true;
        }
      }
      if(err){
        tclistdel(rv);
        rv = NULL;
      }
    } else {
      rv = NULL;
    }
    break;
  case ADBOFDB:
    if(!strcmp(name, "putlist")){
      rv = tclistnew();
      bool err = false;
      argc--;
      for(int i = 0; i < argc; i += 2){
        int ksiz;
        const char *kbuf = tclistval(args, i, &ksiz);
        int vsiz;
        const char *vbuf = tclistval(args, i + 1, &vsiz);
        if(!tcfdbput2(adb->fdb, kbuf, ksiz, vbuf, vsiz)){
          err = true;
          break;
        }
      }
      if(err){
        tclistdel(rv);
        rv = NULL;
      }
    } else if(!strcmp(name, "outlist")){
      rv = tclistnew();
      bool err = false;
      for(int i = 0; i < argc; i++){
        int ksiz;
        const char *kbuf = tclistval(args, i, &ksiz);
        if(!tcfdbout2(adb->fdb, kbuf, ksiz) && tcfdbecode(adb->fdb) != TCENOREC){
          err = true;
          break;
        }
      }
      if(err){
        tclistdel(rv);
        rv = NULL;
      }
    } else if(!strcmp(name, "getlist")){
      rv = tclistnew2(argc);
      bool err = false;
      for(int i = 0; i < argc; i++){
        int ksiz;
        const char *kbuf = tclistval(args, i, &ksiz);
        int vsiz;
        char *vbuf = tcfdbget2(adb->fdb, kbuf, ksiz, &vsiz);
        if(vbuf){
          tclistpush(rv, kbuf, ksiz);
          tclistpush(rv, vbuf, vsiz);
          tcfree(vbuf);
        } else if(tcfdbecode(adb->fdb) != TCENOREC){
          err = true;
        }
      }
      if(err){
        tclistdel(rv);
        rv = NULL;
      }
    } else {
      rv = NULL;
    }
    break;
  default:
    rv = NULL;
    break;
  }
  return rv;
}



/*************************************************************************************************
 * features for experts
 *************************************************************************************************/


/* Get the open mode of an abstract database object. */
int tcadbomode(TCADB *adb){
  assert(adb);
  return adb->omode;
}


/* Get the concrete database object of an abstract database object. */
void *tcadbreveal(TCADB *adb){
  void *rv;
  switch(adb->omode){
  case ADBOMDB:
    rv = adb->mdb;
    break;
  case ADBONDB:
    rv = adb->ndb;
    break;
  case ADBOHDB:
    rv = adb->hdb;
    break;
  case ADBOBDB:
    rv = adb->bdb;
    break;
  case ADBOFDB:
    rv = adb->fdb;
    break;
  default:
    rv = NULL;
    break;
  }
  return rv;
}


/* Process each record atomically of an abstract database object. */
bool tcadbforeach(TCADB *adb, TCITER iter, void *op){
  bool rv;
  switch(adb->omode){
  case ADBOMDB:
    tcmdbforeach(adb->mdb, iter, op);
    rv = true;
    break;
  case ADBONDB:
    tcndbforeach(adb->ndb, iter, op);
    rv = true;
    break;
  case ADBOHDB:
    rv = tchdbforeach(adb->hdb, iter, op);
    break;
  case ADBOBDB:
    rv = tcbdbforeach(adb->bdb, iter, op);
    break;
  case ADBOFDB:
    rv = tcfdbforeach(adb->fdb, iter, op);
    break;
  default:
    rv = false;
    break;
  }
  return rv;
}



// END OF FILE
