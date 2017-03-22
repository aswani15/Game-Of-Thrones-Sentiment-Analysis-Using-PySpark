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

i=0
n=37
chardf <- data.frame()
charmoddf <- data.frame()
for (k in 1:n){
  i=i+1
  url1 = paste0("http://gameofthrones.wikia.com/wiki/Category:Status:_Dead?page=",k)
  page1 = read_html(url1)
  charactercheck = html_text(html_nodes(page1,".title"))
  charmoddf = data.frame(charactercheck)
  if (i==1) {
    chardf <- data.frame(charactercheck)
  }
  else{
    chardf = rbind(chardf,charmoddf)
  }
 }
gotdfdead <- data.frame()
  
  for (p in 568:nrow(chardf)) {
    urldet = paste(gsub(" ","",paste('http://gameofthrones.wikia.com/',gsub(" ","_",chardf[p,1]))))
    print (urldet)
    page2 = read_html(paste0(urldet)) 
    colnamenode = html_nodes(page2, "h3[class='pi-data-label pi-secondary-font']")
    colnames = html_text(colnamenode)
    for (j in 3:length(colnames)+2) {
    key = paste0('.pi-border-color:nth-child(',j,') a')
    colnametxt = paste0('.pi-border-color:nth-child(',j,') .pi-secondary-font')
    colnamesused = toString(text.clean(gsub("\n",'',html_text(html_nodes(page2,colnametxt)))))
   
    if (nchar(colnamesused) > 1 ){
    gotdfdead[p,c(colnamesused)]  = paste(as.character(text.clean(html_text(html_nodes(page2,key)))), collapse=" : ")
    }
    }
    quotes  = paste(as.character(text.clean(html_text(html_nodes(page2,'dd')))), collapse=" : ")
    charactername =  text.clean(gsub("\n",'',html_text(html_nodes(page2,'.pi-title'))))
    seasonsappearance  = paste(as.character(text.clean(html_text(html_nodes(page2,'.appearances td')))), collapse=" : ")
    if (length(charactername)>0){ if (nchar(charactername)>1){
    gotdfdead[p,c("GOTCharacter")]= charactername
    gotdfdead[p,c("quotesUsed")]= quotes
    gotdfdead[p,c("SeasonsAppeared")]= seasonsappearance
     }
    }
    
  }
#setwd("C:/Users/krishna/Desktop/ISB_CBA/BigData/Project")
write.csv(gotdfdead,file="GOTCharactersDead.csv")
