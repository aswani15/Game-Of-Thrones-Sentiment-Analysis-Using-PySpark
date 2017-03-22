require(rvest) || install.packages("rvest")
library("rvest")

require(stringr) || install.packages("stringr")
library(stringr)


library(RCurl)
library(data.table)

text.clean = function(x)                    # text data
{ require("tm")
  x  =  gsub("<.*?>", " ", x)               # regex for removing HTML tags
  x  =  iconv(x, "latin1", "ASCII", sub="") # Keep only ASCII characters
  x  =  gsub("[^[:alnum:]]", " ", x)        # keep only alpha numeric 
  x  =  tolower(x)                          # convert to lower case characters
 # x  =  removeNumbers(x)                    # removing numbers
  x  =  stripWhitespace(x)                  # removing white space
  x  =  gsub("^\\s+|\\s+$", "", x)          # remove leading and trailing white space
  return(x)
}

# 
# 
# 
# tl <- function(e) { if (is.null(e)) return(NULL); ret <- typeof(e); if (ret == 'list' && !is.null(names(e))) ret <- list(type='namedlist') else ret <- list(type=ret,len=length(e)); ret; };
# mkcsv <- function(v) paste0(collapse=',',v);
# keyListToStr <- function(keyList) paste0(collapse='','/',sapply(keyList,function(key) if (is.null(key)) '*' else paste0(collapse=',',key)));
# 
# extractLevelColumns <- function(
#   nodes, ## current level node selection
#   ..., ## additional arguments to data.frame()
#   keyList=list(), ## current key path under main list
#   sep=NULL, ## optional string separator on which to join multi-element vectors; if NULL, will leave as separate columns
#   mkname=function(keyList,maxLen) paste0(collapse='.',if (is.null(sep) && maxLen == 1L) keyList[-length(keyList)] else keyList) ## name builder from current keyList and character vector max length across node level; default to dot-separated keys, and remove last index component for scalars
# ) {
#   cat(sprintf('extractLevelColumns(): %s\n',keyListToStr(keyList)));
#   if (length(nodes) == 0L) return(list()); ## handle corner case of empty main list
#   tlList <- lapply(nodes,tl);
#   typeList <- do.call(c,lapply(tlList,`[[`,'type'));
#   if (length(unique(typeList)) != 1L) stop(sprintf('error: inconsistent types (%s) at %s.',mkcsv(typeList),keyListToStr(keyList)));
#   type <- typeList[1L];
#   if (type == 'namedlist') { ## hash; recurse
#     allKeys <- unique(do.call(c,lapply(nodes,names)));
#     ret <- do.call(c,lapply(allKeys,function(key) extractLevelColumns(lapply(nodes,`[[`,key),...,keyList=c(keyList,key),sep=sep,mkname=mkname)));
#   } else if (type == 'list') { ## array; recurse
#     lenList <- do.call(c,lapply(tlList,`[[`,'len'));
#     maxLen <- max(lenList,na.rm=T);
#     allIndexes <- seq_len(maxLen);
#     ret <- do.call(c,lapply(allIndexes,function(index) extractLevelColumns(lapply(nodes,function(node) if (length(node) < index) NULL else node[[index]]),...,keyList=c(keyList,index),sep=sep,mkname=mkname))); ## must be careful to translate out-of-bounds to NULL; happens automatically with string keys, but not with integer indexes
#   } else if (type%in%c('raw','logical','integer','double','complex','character')) { ## atomic leaf node; build column
#     lenList <- do.call(c,lapply(tlList,`[[`,'len'));
#     maxLen <- max(lenList,na.rm=T);
#     if (is.null(sep)) {
#       ret <- lapply(seq_len(maxLen),function(i) setNames(data.frame(sapply(nodes,function(node) if (length(node) < i) NA else node[[i]]),...),mkname(c(keyList,i),maxLen)));
#     } else {
#       ## keep original type if maxLen is 1, IOW don't stringify
#       ret <- list(setNames(data.frame(sapply(nodes,function(node) if (length(node) == 0L) NA else if (maxLen == 1L) node else paste(collapse=sep,node)),...),mkname(keyList,maxLen)));
#     }; ## end if
#   } else stop(sprintf('error: unsupported type %s at %s.',type,keyListToStr(keyList)));
#   if (is.null(ret)) ret <- list(); ## handle corner case of exclusively empty sublists
#   ret;
# }; ## end extractLevelColumns()
# 
# flattenList <- function(mainList,...) do.call(cbind,extractLevelColumns(mainList,...));


#counts = c(100)#,2,3,4,5,6,7,8,9)
i=0
urlmain = paste0("http://gameofthrones.wikia.com/wiki/Game_of_Thrones_Wiki")
pagemain = read_html(urlmain)
gotdf <- data.frame()
  detailnode =html_nodes(pagemain, "a[class=subnav-3a]")
  detailurl = html_attr(detailnode,"href")
  detailurl = detailurl[104:142]
  detailids = paste('http://gameofthrones.wikia.com',detailurl)
  print (detailids[1])

  for (p in 1:length(detailids)) {
    #length(detailids)){
    urldet = gsub(" ","",(detailids[p]))
    print (urldet)
    page2 = read_html(paste0(urldet))
    colnamenode = html_nodes(page2, "h3[class='pi-data-label pi-secondary-font']")
    colnames = html_text(colnamenode)
    for (j in 4:length(colnames)+2) {
    key = paste0('.pi-border-color:nth-child(',j,') a')
    colnametxt = paste0('.pi-border-color:nth-child(',j,') .pi-secondary-font')
    colnamesused = toString(text.clean(gsub("\n",'',html_text(html_nodes(page2,colnametxt)))))
    gotdf[p,c(colnamesused)]  = paste(as.character(text.clean(html_text(html_nodes(page2,key)))), collapse=" : ")
    }
    quotes  = paste(as.character(text.clean(html_text(html_nodes(page2,'.quote i')))), collapse=" : ")
    charactername =  text.clean(gsub("\n",'',html_text(html_nodes(page2,'.pi-title'))))
    seasonsappearance  = paste(as.character(text.clean(html_text(html_nodes(page2,'.appearances td')))), collapse=" : ")
    gotdf[p,c("GOTCharacter")]= charactername
    gotdf[p,c("quotesUsed")]= quotes
    gotdf[p,c("SeasonsAppeared")]= seasonsappearance
    
  }
#setwd("C:/Users/krishna/Desktop/ISB_CBA/BigData/Project")
write.csv(gotdf,file="GOTCharactersAlive.csv")
