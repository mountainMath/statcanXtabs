#' convert csv to sqlite
#' adapted from https://rdrr.io/github/coolbutuseless/csv2sqlite/src/R/csv2sqlite.R
#'
#' @param csv_file input csv path
#' @param sqlite_file output sql database path
#' @param table_name sql table name
#' @param transform optional function that transforms each chunk
#' @param chunk_size optional chunk size to read/write data, default=1,000,000
#' @param append optional parameter, append to database or overwrite, defaul=`FALSE`
#' @param col_types optional parameter for csv column types
#' @param na na character strings
#' @param text_encoding encoding of csv file (default UTF8)
#' @export
csv2sqlite <- function(csv_file, sqlite_file, table_name, transform=NULL,chunk_size=5000000,
                       append=FALSE,col_types=NULL,na=c(NA,"..","","...","F"),
                       text_encoding="UTF8") {
   # Connect to database.
  if (!append && file.exists(sqlite_file)) file.remove(sqlite_file)
  con <- DBI::dbConnect(RSQLite::SQLite(), dbname=sqlite_file)

  chunk_handler <- function(df, pos) {
    if (nrow(readr::problems(df)) > 0) print(readr::problems(df))
    if (!is.null(transform)) df <- df %>% transform
    DBI::dbWriteTable(con, table_name, as.data.frame(df), append=TRUE)
  }

  readr::read_csv_chunked(csv_file,
                          callback=readr::DataFrameCallback$new(chunk_handler),
                          col_types=col_types,
                          chunk_size = chunk_size,
                          locale=readr::locale(encoding = text_encoding),
                          na=na)

  DBI::dbDisconnect(con)
}


#' convert tibble or data frame to sqlite
#'
#' @param data input tibble or data frame
#' @param sqlite_file output sql database path
#' @param table_name sql table name
#' @param append optional parameter, append to database or overwrite, defaul=`FALSE`
#'
#' @export
data2sqlite <- function(data, sqlite_file, table_name, append=FALSE) {
  if (!append && file.exists(sqlite_file)) file.remove(sqlite_file)
  con <- DBI::dbConnect(RSQLite::SQLite(), dbname=sqlite_file)
  DBI::dbWriteTable(con, table_name, as.data.frame(data), append=TRUE)
  DBI::dbDisconnect(con)
}




#' remove notes
#' @export
remove_xtab_notes_transform <- function(data){
  data %>% dplyr::select(names(.)[!grepl("Notes: ",names(.))])
}

#' transform function for get_sqlite_xtab cleaning names
#' @export
clean_xtab_names_transform <- function(data){
  data %>%
    rlang::set_names(gsub(" \\(Note: \\d+\\)$","",names(.))) %>%
    rlang::set_names(gsub("^DIM: | \\(\\d+[A-Z]*\\)$","",names(.)))
}

#' transform function for get_sqlite_xtab cleaning names
#' @export
numeric_xtab_values_transfrom <- function(data) {
  value_columns <- names(data)[grepl("^Dim: ",names(data))]
  data %>% mutate_at(value_columns,as.numeric)
}

#' transform function for get_sqlite_xtab cleaning names
#' @export
standard_xtab_transform <- function(data,remove_member_id=TRUE){
  data %>%
    remove_xtab_notes_transform %>%
    clean_xtab_names_transform %>%
    numeric_xtab_values_transfrom
}

#' transforms standard StatCan xtab to long form
#' @param data data from get_sqlite_xtab after calling collect()
#' @export
tidy_xtab <- function(data,remove_member_id=FALSE){
  if (!tibble::is_tibble(data)) stop("This function only works on tibbles. Please call `collect()` first.")
  value_columns <- names(data)[grepl("^Dim: ",names(data))]
  gather_name <- value_columns %>%
    gsub(": Member ID: .+$","",.) %>%
    unique %>%
    gsub("^Dim: ","",.) %>%
    gsub(" \\(Note: \\d+\\)$","",.) %>%
    gsub(" \\(\\d+[A-Z]*\\)$","",.)
  if (length(gather_name)!=1) stop("Could not parse out value columns")
  clean_value_columns <- value_columns %>%
    gsub("^.+: Member ID: ","Member ID: ",.)
  if (remove_member_id) clean_value_columns <- gsub("Member ID: \\[\\d+[A-Z]*\\]: ","",clean_value_columns)
  data %>%
    rename(!!!set_names(value_columns,clean_value_columns)) %>%
    gather(key=!!gather_name,value="Value",clean_value_columns)
}


get_tmp_zip_archive <- function(url,exdir=tempdir()){
  message("Downloading data ...")
  temp=tempfile(fileext = ".zip")
  download.file(url,temp)
  message("Unpacking data ...")
  zipped_info <- zip::zip_list(temp) %>% mutate(Name=filename,Length=uncompressed_size)
  # unzip_type=ifelse(is.null(getOption("unzip")),"internal",getOption("unzip"))
  # zipped_info <- utils::unzip(temp, list=TRUE,unzip=unzip_type)
  # large <- sum(zipped_info$Length) > 4000000000
  # if (large && unzip_type=="internal")
  #   message(paste0("File might be too large for R internal unzip.\n",
  #                  "If unzip fails, consider setting the 'R_UNZIPCMD' environment vairable or\n",
  #                  "downlod and unzip the file manually and use the 'existing_unzip_path' parameter.\n",
  #                  "Downloaded archive is at ",temp))
  #utils::unzip(temp,exdir=exdir,unzip=unzip_type)
  zip::unzip(temp,exdir=exdir)
  unlink(temp)
  zipped_info
}


parse_xml_xtab <- function(structure_path,generic_path){
  xml_structure <- read_xml(structure_path)
  xml_data <- read_xml(generic_path)
  str_concepts <- xml_structure %>% xml_find_all("//structure:ConceptScheme")
  var_concepts <- str_concepts %>%
    xml_find_all("structure:Concept") %>%
    xml_attr("id") %>% setdiff(c("TIME", "GEO", "OBS_VALUE", "OBS_STATUS" ))
  concepts <- c("GEO",var_concepts)

  concept_names <- lapply(concepts,function(c){
    xml_structure %>% xml_find_all(paste0("//structure:ConceptScheme/structure:Concept[@id='",c,"']/structure:Name[@xml:lang='en']")) %>%
      xml_text}) %>% unlist

  concept_lookup <- rlang::set_names(concept_names,concepts)

  descriptions_for_code <- function(c){
    c <- gsub("0$","",c)
    base <- xml_structure %>% xml_find_all(paste0("//structure:CodeList[@id='CL_",toupper(c),"']/structure:Code"))
    desc_text = ifelse(length(xml_find_all(base[1] ,".//structure:Description[@xml:lang='en']"))==0,".//structure:Description",".//structure:Description[@xml:lang='en']")
    rlang::set_names(
      base %>% purrr::map(function(e){e %>% xml_find_all(desc_text) %>% xml_text}) %>% unlist %>% trimws(),
      base %>% purrr::map(function(e){xml_attr(e,"value")}) %>% unlist
    )
  }

  series <- xml_find_all(xml_data,"//generic:Series")

  #series <- series[1:10]

  #l <- lapply(concepts,function(c){series %>% xml_find_all(paste0(".//generic:Value[@concept='",c,"']")) %>% xml_attr("value")})

  time_data <- function(series){
    message("Extracting year data")
    series %>% xml_find_all(".//generic:Time") %>% xml_text
  }
  value_data <- function(series){
    message("Extracting values data")
    #series %>% xml_find_all(".//generic:ObsValue") %>% xml_attr("value")
    series %>% xml_find_all("./generic:Obs") %>% xml_find_first("./generic:ObsValue") %>% xml_attr("value")
  }
  code_data <- function(series,c){
    message(paste0("Extracting data for ",concept_lookup[c]))
    series %>% xml_find_all(paste0(".//generic:Value[@concept='",c,"']")) %>% xml_attr("value")
  }

  df=concepts %>%
    purrr::map(function(c){code_data(series,c)}) %>%
    rlang::set_names(concept_names) %>%
    tibble::as_tibble() %>%
    dplyr::bind_cols(tibble::tibble(Year = time_data(series), Value = value_data(series)))

  for (i in seq(1,length(concepts),1)) {
    c=concepts[i]
    n=concept_names[i] %>% as.name
    lookup <- descriptions_for_code(c)
    nid=paste0(n," ID") %>% as.name
    df <- df %>%
      mutate(!!nid := !!n) %>%
      mutate(!!n := lookup[!!nid])
  }

  fix_ct_geo_format <- function(geo){
    ifelse(nchar(geo)==9,paste0(substr(geo,1,7),".",substr(geo,8,9)),geo)
  }

  df  %>%
    dplyr::rename(GeoUID=`Geography ID`) %>%
    dplyr::mutate(GeoUID = fix_ct_geo_format(GeoUID),
           Value=as.numeric(Value))
}


create_index <- function(connection,table_name,field){
  field_index=paste0("index_",gsub("[^[:alnum:]]","_",field))
  query=paste0("CREATE INDEX IF NOT EXISTS ",field_index," ON ",table_name," (`",field,"`)")
  #print(query)
  r<-DBI::dbSendQuery(connection,query)
  DBI::dbClearResult(r)
  NULL
}

#' create indices on index_fields in the sqlite database in case they don't exist
#' @param sqlite_path path to the sqlite database
#' @param table_name name of the table
#' @param index_field optional vector of column names to index, defaul indexes all columns
#'
#' @export
index_xtab_fields <- function(sqlite_path,table_name,index_fields=NULL){
  con <- DBI::dbConnect(RSQLite::SQLite(), dbname=sqlite_path)
  fields <- DBI::dbListFields(con,table_name)
  if (!is.null(index_fields)) {
    fields=intersect(fields,index_fields)
    if (length(fields)!=length(index_fields)) {
      warning(paste0("Could not find fields ",paste0(setdiff(index_fields,fields),collapse = ", ")))
    }
  }
  lapply(fields,function(field)create_index(con,table_name,field))
  DBI::dbDisconnect(con)
}

#' close_sqlite_xtab
#'
#' @param xtab connection to the xtab as retured from get_sqlite_xtab
#' @export
close_sqlite_xtab <- function(xtab){
  DBI::dbDisconnect(xtab$src$con)
}

#' get_sqlite_xtab
#'
#' @param code Code for the xtab, used for local caching
#' @param url url to download the csv version of the xtab
#' @param cache_dir optional cache directory.
#' @param existing_unzip_path optional path to unzipped xtab, useful for large (>4GB unzipped) xtabs on windows machines
#' @export
#'
#' @usage  get_sqlite_xtab("98-400-X2016288","https://www12.statcan.gc.ca/census-recensement/2016/dp-pd/dt-td/CompDataDownload.cfm?LANG=E&PID=111849&OFT=CSV")
get_sqlite_xtab <- function(code,
                            url=NA,
                            cache_dir=getOption("statcanXtab.cache_dir"),
                            transform=standard_xtab_transform,
                            refresh=FALSE,
                            existing_unzip_path=NA,
                            format="csv"){
  if (is.null(cache_dir)) stop("Cache directory needs to be set!")
  sqlite_path <- file.path(cache_dir,paste0(code,".sqlite"))
  if (refresh || !file.exists(sqlite_path)) {
    if (!is.na(existing_unzip_path)) {
      message("Locating xtab ...")
      if (dir.exists(existing_unzip_path)) {
        zipped_info <- dir(existing_unzip_path)
        data_file=zipped_info[grepl("_data\\.csv$",zipped_info)]
        meta_file=zipped_info[grepl("_meta\\.txt$",zipped_info)& !grepl("README",zipped_info)]
        if (length(data_file)==0) {
          stop(paste0("No xtabs found in ",existing_unzip_path,"."))
        } else if (length(data_file)>1) {
          stop(paste0("Found ",length(data_file)," xtab files in ",existing_unzip_path,".\n",
                      "Don't know which file to choose: ",paste0(data_file,collapse=", "),".\n",
                      "Please use a separate directory for each xtab."))
        }
        data_file <- file.path(existing_unzip_path,data_file)
        meta_file <- file.path(existing_unzip_path,meta_file)
        zipped_info=NULL
      } else {
        if (file.exists(existing_unzip_path)) {
          data_file=existing_unzip_path
          meta_file=NULL
          zipped_info=NULL
        }
      }
    } else {
      if (is.na(url)) stop("Need either url or existing_unzip_path set!")
      exdir=tempdir()
      if (format != "python_xml") {
        zipped_info <- get_tmp_zip_archive(url,exdir)$Name
        data_file=file.path(exdir,zipped_info[grepl("_data\\.csv$",zipped_info)])
        meta_file=file.path(exdir,zipped_info[grepl("_meta\\.txt$",zipped_info)& !grepl("README",zipped_info)])
      }
    }
    message("Importing xtab ...")
    table_name="xtab_data"
    if (format=='xml') {
      clean_xml_names <- function(data){
        n <- names(data)
        n[grepl(" ID$",n)]=paste0("Member ID: ",gsub(" ID$","",n[grepl(" ID$",n)]))
        rlang::set_names(data,n)
        data
      }
      exdir=tempdir()
      structure_path=file.path(exdir,zipped_info[grepl("Structure_.*\\.xml$",zipped_info)])
      generic_path=file.path(exdir,zipped_info[grepl("Generic_.*\\.xml$",zipped_info)])
      df <- parse_xml_xtab(structure_path,generic_path) %>%
        clean_xml_names %>%
        transform
      con <- DBI::dbConnect(RSQLite::SQLite(), dbname=sqlite_path)
      DBI::dbWriteTable(conn=con, name=table_name, value=df, append=FALSE,overwrite=TRUE)
      DBI::dbDisconnect(con)
      index_fields <- setdiff(names(df),"Value")
    } else if (format =='csv') {
      if (length(data_file)!=1) stop(paste0("Could not find unique file to extract: ",paste0(data_file,collapse = ", ")))
      if (!is.null(meta_file)) {
        meta_lines <- readr::read_lines(meta_file)
        meta_path <- file.path(cache_dir,paste0(code,"_meta.txt"))
        readr::write_lines(meta_lines,meta_path)
      }
      csv2sqlite(
        csv_file=file.path(data_file),
        sqlite_file=sqlite_path,
        table_name=table_name,
        col_types = readr::cols(.default = "c"),
        transform = transform)
      index_fields = readr::read_csv(file.path(data_file),n_max=1,col_types=readr::cols(.default = "c")) %>%
        transform %>%
        dplyr::select(names(.)[grepl("^DIM: |GEO_CODE \\(POR\\)|ALT_GEO_CODE|CENSUS_YEAR|GeoUID|GEO_NAME",names(.))]) %>%
        names
    } else if (format=="python_xml") {
      csv_path<-statcanXtabs::xml_to_csv(code,url,refresh = refresh)
      csv2sqlite(csv_path,sqlite_path,table_name,col_types = readr::cols(.default = "c"))
      index_fields <- read_csv(csv_path,n_max = 1) %>% select(-matches(" ID$|^Value$")) %>% names()
      index_xtab_fields(sqlite_path,table_name,index_fields)
      file.remove(csv_path)
    } else {
      stop(paste0("Don't know how to import format ",format,"."))
    }
    if (!is.null(zipped_info)) zipped_info %>% file.path(exdir,.) %>% lapply(unlink)
    print("Indexing xtab...")
    index_xtab_fields(sqlite_path,table_name,index_fields)
  } else {
    message("Opening local sqlite xtab...")
  }
  DBI::dbConnect(RSQLite::SQLite(), sqlite_path) %>%
    dplyr::tbl("xtab_data")
}

xtab_download_url_for <- function(code) {
  url=paste0("http://www12.statcan.gc.ca/global/URLRedirect.cfm?lang=E&ips=",code)
  page <- httr::GET(url)
  redirect <- page$all_headers[[2]]$headers$location
}

get_xtab_list <- function(year=2016) {
  url="https://www150.statcan.gc.ca/n1/en/catalogue/98-400-X"
  r<-httr::GET(url)
  data <- httr::content(r)
  table <- rvest::html_nodes(data,".pane-ndm-releases table")
  rows <- rvest::html_nodes(table,"tr")
}

#' get xtab in long form
#' @export
standardize_xtab <- function(data){
  ns <- names(data)[grepl("^Dim: ",names(data))]
  grep_string <- paste0(gsub(" \\(\\d+\\): .+$","",ns[1])," \\(\\d+\\): Member ID: \\[\\d+\\]: ")
  key_string <- paste0(gsub("^Dim: ","",gsub(" \\(\\d+\\): .+$","",ns[1])))

  data %>%
    gather_for_grep_string(key_string,grep_string) %>%
    rlang::set_names(gsub("^DIM: | \\(\\d+\\)$","",names(.))) %>%
    strip_columns_for_grep_string("^Notes: ") %>%
    dplyr::mutate(Value=as.numeric(Value,na=c("x", "F", "...", "..",NA)))
}


#' Download xml xtab data from url, tag with code for caching
#' useful for older (2006) data that does not come as csv option
#'
#' @param code The statcan table code, needed for caching and for locating the table to extract
#' @param url The url to the xml table download. Only needed if the table is not cached, but
#' reommended to always include for reproducibility
#' @param refresh Will refresh cached data if set to TRUE, default is FALSE
#' @param time_value If set, will use this as the time variable, otherwise extract the time variable from the data.
#' Setting this will slightly speed up the parsing.
#' @param temp A path to the downloaded xml zip file. Useful if file has already been downloaded and should not be
#' downloaded again.
#'
#' @export
xml_xtab_for <- function(code,url,refresh=FALSE,time_value=NA,temp=NA,system_unzip=FALSE){
  data <- NA
  path <- file.path(getOption("custom_data_path"),paste0(code,".rda"))
  if (!file.exists(path) | refresh) {
    exdir <- tempdir()
    if (is.na(temp)) {
      temp <- tempfile()
      download.file(url,temp)
      unzip_file(temp,exdir=exdir,system_unzip = TRUE)
      unlink(temp)
    } else {
      unzip_file(temp,exdir=exdir,system_unzip = TRUE)
    }
    # read xml xtab for 2006 data.
    message("Parsing XML")
    xml_structure <- read_xml(file.path(exdir,paste0("Structure_",code,".xml")))
    xml_data <- read_xml(file.path(exdir,paste0("Generic_",code,".xml")))
    unlink(exdir,recursive = TRUE)

    str_concepts <- xml_structure %>% xml_find_all("//structure:ConceptScheme")
    var_concepts <- str_concepts %>%
      xml_find_all("structure:Concept") %>%
      xml_attr("id") %>% setdiff(c("TIME", "GEO", "OBS_VALUE", "OBS_STATUS" ))
    concepts <- c("GEO",var_concepts)

    concept_names <- lapply(concepts,function(c){
      xml_structure %>% xml_find_all(paste0("//structure:ConceptScheme/structure:Concept[@id='",c,"']/structure:Name[@xml:lang='en']")) %>%
        xml_text}) %>% unlist

    concept_lookup <- rlang::set_names(concept_names,concepts)

    descriptions_for_code <- function(c){
      c <- gsub("0$","",c)
      base <- xml_structure %>% xml_find_all(paste0("//structure:CodeList[@id='CL_",toupper(c),"']/structure:Code"))
      desc_text = ifelse(length(xml_find_all(base[1] ,".//structure:Description[@xml:lang='en']"))==0,".//structure:Description",".//structure:Description[@xml:lang='en']")
      rlang::set_names(
        base %>% purrr::map(function(e){e %>% xml_find_all(desc_text) %>% xml_text}) %>% unlist %>% trimws(),
        base %>% purrr::map(function(e){xml_attr(e,"value")}) %>% unlist
      )
    }

    series <- xml_find_all(xml_data,"//generic:Series")

    #series <- series[1:10]

    #l <- lapply(concepts,function(c){series %>% xml_find_all(paste0(".//generic:Value[@concept='",c,"']")) %>% xml_attr("value")})

    time_data <- function(series){
      if (!is.na(time_value)) {
        result=rep(time_value,length(series))
      } else {
        message("Extracting year data")
        series %>% xml_find_all(".//generic:Time") %>% xml_text
      }
    }
    value_data <- function(series){
      message("Extracting values data")
      #series %>% xml_find_all(".//generic:ObsValue") %>% xml_attr("value")
      series %>% xml_find_all("./generic:Obs") %>% xml_find_first("./generic:ObsValue") %>% xml_attr("value")
    }
    code_data <- function(series,c){
      message(paste0("Extracting data for ",concept_lookup[c]))
      series %>% xml_find_all(paste0(".//generic:Value[@concept='",c,"']")) %>% xml_attr("value")
    }

    df=concepts %>%
      purrr::map(function(c){code_data(series,c)}) %>%
      rlang::set_names(concept_names) %>%
      tibble::as.tibble() %>%
      dplyr::bind_cols(tibble::tibble(Year = time_data(series), Value = value_data(series)))

    for (i in seq(1,length(concepts),1)) {
      c=concepts[i]
      n=concept_names[i] %>% as.name
      lookup <- descriptions_for_code(c)
      nid=paste0(n," ID") %>% as.name
      df <- df %>%
        mutate(!!nid := !!n) %>%
        mutate(!!n := lookup[!!nid])
    }

    fix_ct_geo_format <- function(geo){
      ifelse(nchar(geo)==9,paste0(substr(geo,1,7),".",substr(geo,8,9)),geo)
    }

    data <- df  %>%
      rename(GeoUID=`Geography ID`) %>%
      mutate(GeoUID = fix_ct_geo_format(GeoUID),
             Value=as.numeric(Value))

    saveRDS(data,path)
  } else {
    data <- readRDS(path)
  }
  data
}

#' Download xml xtab data from url, tag with code for caching
#' useful for (2001) census profile data
#' @export
xml_census_2001_profile <- function(code,url,refresh=FALSE,time_value=NA,temp=NA){
  data <- NA
  path <- file.path(getOption("custom_data_path"),paste0(code,".rda"))
  if (!file.exists(path) | refresh) {
    exdir <- tempdir()
    if (is.na(temp)) {
      temp <- tempfile()
      download.file(url,temp)
      utils::unzip(temp,exdir=exdir)
      unlink(temp)
    } else {
      utils::unzip(temp,exdir=exdir)
    }
    # read xml xtab for 2006 data.
    message("Parsing XML")
    xml_structure <- read_xml(file.path(exdir,paste0("Structure_",code,".xml")))
    xml_data <- read_xml(file.path(exdir,paste0("Generic_",code,".xml")))

    str_concepts <- xml_structure %>% xml_find_all("//structure:ConceptScheme")
    var_concepts <- str_concepts %>%
      xml_find_all("structure:Concept") %>%
      xml_attr("id") %>% setdiff(c("TIME", "GEO", "OBS_VALUE", "OBS_STATUS" ))
    concepts <- c("GEO",var_concepts)

    concept_names <- lapply(concepts,function(c){
      xml_structure %>% xml_find_all(paste0("//structure:ConceptScheme/structure:Concept[@id='",c,"']/structure:Name[@xml:lang='en']")) %>%
        xml_text}) %>% unlist

    concept_lookup <- rlang::set_names(concept_names,concepts)

    descriptions_for_code <- function(c){
      c <- gsub("0$","",c)
      base <- xml_structure %>% xml_find_all(paste0("//structure:CodeList[@id='CL_",toupper(c),"']/structure:Code"))
      desc_text = ifelse(length(xml_find_all(base[1] ,".//structure:Description[@xml:lang='en']"))==0,".//structure:Description",".//structure:Description[@xml:lang='en']")
      rlang::set_names(
        base %>% purrr::map(function(e){e %>% xml_find_all(desc_text) %>% xml_text}) %>% unlist %>% trimws(),
        base %>% purrr::map(function(e){xml_attr(e,"value")}) %>% unlist
      )
    }

    series <- xml_find_all(xml_data,"//generic:Series")

    #series <- series[1:10]

    #l <- lapply(concepts,function(c){series %>% xml_find_all(paste0(".//generic:Value[@concept='",c,"']")) %>% xml_attr("value")})

    time_data <- function(series){
      if (!is.na(time_value)) {
        result=rep(time_value,length(series))
      } else {
        message("Extracting year data")
        series %>% xml_find_all(".//generic:Time") %>% xml_text
      }
    }
    value_data <- function(series){
      message("Extracting values data")
      #series %>% xml_find_all(".//generic:ObsValue") %>% xml_attr("value")
      series %>% xml_find_all("./generic:Obs") %>% xml_find_first("./generic:ObsValue") %>% xml_attr("value")
    }
    code_data <- function(series,c){
      message(paste0("Extracting data for ",concept_lookup[c]))
      series %>% xml_find_all(paste0(".//generic:Value[@concept='",c,"']")) %>% xml_attr("value")
    }

    df=concepts %>%
      purrr::map(function(c){code_data(series,c)}) %>%
      rlang::set_names(concept_names) %>%
      tibble::as.tibble()  %>%
      dplyr::bind_cols(tibble::tibble(Year = time_data(series), Value = value_data(series)))

    for (i in seq(1,length(concepts),1)) {
      c=concepts[i]
      n=concept_names[i] %>% as.name
      lookup <- descriptions_for_code(c)
      nid=paste0(n," ID") %>% as.name
      df <- df %>%
        mutate(!!nid := !!n) %>%
        mutate(!!n := lookup[!!nid])
    }

    fix_ct_geo_format <- function(geo){
      ifelse(nchar(geo)==9,paste0(substr(geo,1,7),".",substr(geo,8,9)),geo)
    }

    data <- df  %>%
      rename(GeoUID=`Geography ID`) %>%
      mutate(GeoUID = fix_ct_geo_format(GeoUID),
             Value=as.numeric(Value))

    unlink(exdir,recursive = TRUE)
    saveRDS(data,path)
  } else {
    data <- readRDS(path)
  }
  data
}

#' Set up virtual conda env
#' @export
install_python_environment <- function(python_path="/opt/anaconda3/bin/python3.7",conda="/opt/anaconda3/bin/conda"){
  reticulate::use_python(python_path)
  #reticulate::conda_binary(conda = python_path)
  #reticulate::conda_remove("r-reticulate")
  reticulate::use_condaenv("r-reticulate",conda=conda)
  reticulate::conda_create("r-reticulate",packages="lxml",conda=conda)
}

#' Use python to parse xml
#' @export
xml_to_csv <- function(code,url,python_path="/opt/anaconda3/bin/python3.7",refresh=FALSE,time_value=NA,temp=NA,
                       conda="/opt/anaconda3/bin/conda"){
  path <- file.path(getOption("custom_data_path"),paste0(code,".csv"))
  if (refresh | !file.exists(path)) {
    remove_temp=FALSE
    if (is.na(temp)) {
      message("Downloading file")
      remove_temp=TRUE
      temp <- tempfile(".zip")
      download.file(url,temp)
    }
    message("Converting xml to csv")
    reticulate::use_python(python_path)
    reticulate::conda_binary(conda = python_path)
    reticulate::use_condaenv("r-reticulate",conda=conda)
    statcan = reticulate::import_from_path("statcan",file.path(system.file(package="statcanXtabs")))
    statcan$convert_statcan_xml_to_csv(temp,path)
    if (remove_temp) unlink(temp)
    message("Done converting xml to csv")
  }
  path
}


#' Download xml xtab data from url, tag with code for caching
#' useful for older (2006) data that does not come as csv option
#'
#' @param code The statcan table code, needed for caching and for locating the table to extract
#' @param url The url to the xml table download. Only needed if the table is not cached, but
#' reommended to always include for reproducibility
#' @param python_path path to python executable, expects a conda environment 'r-reticulate' to exist.
#' Can be set up with `install_python_environment`.
#' @param refresh Will refresh cached data if set to TRUE, default is FALSE
#' @param temp A path to the downloaded xml zip file. Useful if file has already been downloaded and should not be
#' downloaded again.
#'
#' @export
xml_via_python <- function(code,url,python_path="/opt/anaconda3/bin/python3.7",conda="/opt/anaconda3/bin/conda",refresh=FALSE,temp=NA){
  readr::read_csv(xml_to_csv(code,url,python_path=python_path,conda=conda,refresh=refresh,temp=temp))
}


#' @export
gather_for_grep_string <- function(data,gather_key,grep_string){
  vars <- names(data)[grepl(grep_string,names(data))]
  short_vars <- gsub(grep_string,"",vars)
  short_vars[duplicated(short_vars)]=tibble(v=short_vars[duplicated(short_vars)]) %>% mutate(n=paste0(v," (",row_number(),")")) %>% pull(n)
  #names(data) <- gsub(grep_string,"",names(data))
  data %>%
    rename(!!!set_names(vars,short_vars)) %>%
    tidyr::gather(key=!!gather_key,value="Value",short_vars)
}

#' @export
strip_columns_for_grep_string <- function(data,grep_string){
  data %>% dplyr::select(names(data)[!grepl(grep_string,names(data))])
}

spark_import <- function(path){
  sc <- sparklyr::spark_connect(master = "local")
  data_raw_spk <- sparklyr::spark_read_csv(
    path = path,
    sc = sc,
    name = "data_export_raw",
    overwrite = TRUE
  )
}



#' @import xml2
#' @importFrom dplyr %>%
#' @importFrom rlang .data
NULL

