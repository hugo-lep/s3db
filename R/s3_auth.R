#S3_ACCESS_KEY_ID_HL
#S3_SECRET_ACCESS_KEY_HL
#S3_BUCKET_NAME_HL
#S3_DEFAULT_REGION_HL
#S3_ENDPOINT_HL

#' Set S3 bucket and main folder for this app
#'
#' @description
#' Cette fonction sert à enregistrer un fichier contenant le bucket et dossier principal lié avec ce projet
#' @param s3_bucket Nom de mon bucket S3
#' @param s3_main_folder Nom du dossier principal dans le bucket
#' @param secure_key Sel pour cripter les informations sensibles
#' @param config_path emplacement pour écrire un fichier de config local
#'
#' @importFrom readr write_rds
#'
#' @return Rien. La fonction enregistre un fichier sur le disque.
#' @export
#'
#' @examples
#' if (interactive()) {
#'   s3_bucket <- "my-bucket"
#'   s3_main_folder <- "my-folder"
#'   secure_key <- "123456"
#'   set_config_s3_location(s3_bucket, s3_main_folder, secure_key)
#' }
set_config_s3_location <- function(s3_bucket, s3_main_folder, secure_key,
                                config_path = "inst/app/data/") {

  stopifnot(
    is.character(s3_bucket), length(s3_bucket) == 1,
    is.character(s3_main_folder), length(s3_main_folder) == 1,
    is.character(secure_key), length(secure_key) == 1
  )

  config_path <- paste0(config_path,"config_s3_location.rds")
  config <- list(
    s3_bucket = s3_bucket,
    s3_main_folder = s3_main_folder,
    secure_key = secure_key
  )

  if (file.exists(config_path)) {
    message("Fichier '", config_path, "' déjà existant")
    message("Ces informations ne devraient pas être modifiées au cours d’un projet")
    message("Pour réinitialiser ces informations, veuillez effacer manuellement ce fichier")
    return(invisible(NULL))
  }

  dir.create(dirname(config_path), recursive = TRUE, showWarnings = FALSE)
  write_rds(config, config_path)

  ligne_a_ajouter <- config_path
  if (file.exists(".gitignore")) {
    lignes <- readLines(".gitignore")
    if (!(ligne_a_ajouter %in% lignes)) {
      write(paste0("\n", ligne_a_ajouter), ".gitignore", append = TRUE)
      message("Fichier '", config_path, "' ajouté à .gitignore")
    } else {
      message("Fichier '", config_path, "' est déjà présent dans .gitignore")
    }
  }
}

#' Save S3 access
#'
#' @description
#' Sert à enregistrer les informations nécessaires pour accéder à S3/AWS
#'
#' @param s3_ACCESS_KEY_ID AWS ACCESS KEY ID
#' @param s3_SECRET_ACCESS_KEY AWS SECRET ACCESS KEY
#' @param s3_REGION REGION
#' @param s3_ENDPOINT Endpoint (à spécifier si autre que AWS/S3)
#' @param config_path emplacement pour écrire un fichier de config local
#'
#' @returns Rien. La fonction enregistre un fichier sur le disque.
#' @importFrom sodium data_encrypt
#' @importFrom sodium hash
#' @export
#'
#' @examples
#' if (interactive()) {
#'   s3_ACCESS_KEY_ID <- "access_key"
#'   s3_SECRET_ACCESS_KEY <- "secret_key"
#'   s3_REGION <- "where"
#'   s3_ENDPOINT <- "end_point"
#'   set_config_s3_access("access_key", "secret_key", "region","end_point","s3_ENDPOINT")
#' }
set_config_s3_access <- function(s3_ACCESS_KEY_ID,
                                 s3_SECRET_ACCESS_KEY,
                                 s3_REGION,
                                 s3_ENDPOINT = "s3.amazonaws.com",
                                 config_path = "inst/app/data/") {

  key_file_path <- paste0(config_path,"config_s3_location.rds")

  if (!file.exists(key_file_path)) {
    stop(paste0("Le fichier 'config_s3_location.rds' est manquant.\n",
                "Appelez d'abord 'protegR_init_s3path()'."))
  }

  secure_key <- readRDS(key_file_path)$secure_key
  if (is.null(secure_key)) stop("La clé de chiffrement (secure_key) est manquante dans config_s3_location.rds")
  secure_key_raw <- sodium::hash(charToRaw(secure_key))

  stopifnot(
    is.character(s3_ACCESS_KEY_ID), length(s3_ACCESS_KEY_ID) == 1,
    is.character(s3_SECRET_ACCESS_KEY), length(s3_SECRET_ACCESS_KEY) == 1,
    is.character(s3_REGION), length(s3_REGION) == 1
  )

  file_path <- paste0(config_path,"config_s3_access.rds")


  if (file.exists(file_path)) {
    message("Fichier '", file_path, "' déjà existant")
    message("Ces informations ne devraient pas être modifiées au cours d’un projet")
    message("Pour réinitialiser ces informations, veuillez effacer manuellement ce fichier")
    return(invisible(NULL))
  }

  data <- list(
    s3_ACCESS_KEY_ID = data_encrypt(msg = charToRaw(s3_ACCESS_KEY_ID), key = secure_key_raw),
    s3_SECRET_ACCESS_KEY = data_encrypt(msg = charToRaw(s3_SECRET_ACCESS_KEY), key = secure_key_raw),
    s3_REGION = s3_REGION,
    s3_ENDPOINT = s3_ENDPOINT
  )

  saveRDS(data, file_path)

  ligne_a_ajouter <- file_path
  if (file.exists(".gitignore")) {
    lignes <- readLines(".gitignore")
    if (!(ligne_a_ajouter %in% lignes)) {
      write(paste0("\n", ligne_a_ajouter), ".gitignore", append = TRUE)
      message("Fichier '", file_path, "' ajouté à .gitignore")
    } else {
      message("Fichier '", file_path, "' est déjà présent dans .gitignore")
    }
  }

}


#' S3 initialisation
#'
#' @param key (chr) S3 key
#' @param secret (chr) s3 secret
#' @param main_folder (chr) folder added behind object
#' @param bucket (chr) s3 bucket
#' @param endpoint (chr) endpoint nécessaire pour accéder à S3
#' @param region (chr) s3 region, OVH == ""
#' @param config_path TRUE,FALSE or write location ex: "inst/config/"
#'
#' @importFrom aws.s3 s3readRDS
#' @importFrom sodium hash data_decrypt
#'
#' @returns Rien, mais crée des variables environnement simplifiant la connexion à S3
#' @export
#'
#' @examples
#' if(interactive()){
#' s3_connection_HL()
#' }
s3_connection_HL <- function(key = NA,
                             secret = NA,
                             main_folder = NA,
                             bucket = NA,
                             endpoint = NA,
                             region = NA,
                             config_path = TRUE) {

  # Déterminer le chemin du fichier de config
  if (isTRUE(config_path)) {
    access_path <- "inst/app/data/config_s3_access.rds"
    location_path <- "inst/app/data/config_s3_location.rds"
  } else if (isFALSE(config_path)) {
    access_path <- NULL  # tout sera fourni manuellement
    location_path <- NULL
  } else if (is.character(config_path)) {
    access_path <- file.path(config_path, "config_s3_access.rds")
    location_path <- file.path(config_path, "config_s3_location.rds")
  } else {
    stop("Argument `config_path` doit être TRUE, FALSE ou un chemin de fichier.")
  }

  # Vérifier l'existence des fichiers si nécessaire
  if (!is.null(access_path)) {
    if (!file.exists(access_path)) stop("Fichier config_s3_access.rds introuvable dans ", access_path)
    if (!file.exists(location_path)) stop("Fichier config_s3_location.rds introuvable dans ", location_path)

    config_s3_access <- readRDS(access_path)
    config_s3_location <- readRDS(location_path)
    secure_key_raw <- hash(charToRaw(config_s3_location$secure_key))

    if (is.na(key))    key      <- rawToChar(data_decrypt(bin = config_s3_access$s3_ACCESS_KEY_ID, key = secure_key_raw))
    if (is.na(secret)) secret   <- rawToChar(data_decrypt(bin = config_s3_access$s3_SECRET_ACCESS_KEY, key = secure_key_raw))
    if (is.na(bucket)) bucket   <- config_s3_location$s3_bucket
    if (is.na(main_folder)) main_folder   <- config_s3_location$s3_main_folder
    if (is.na(region)) region   <- config_s3_access$s3_REGION
    if (is.na(endpoint)) endpoint <- config_s3_access$s3_ENDPOINT
    if (is.na(bucket)) bucket   <- config_s3_location$s3_bucket
  }

  #  Vérification minimale
  if (any(is.na(c(key, secret, bucket, endpoint, region)))) {
    stop("S3 non initialisé correctement. Vérifiez vos variables d'environnement ou les arguments passés.")
  }

  #  Mettre en place les variables environnement HL
  Sys.setenv(
    HL_S3_KEY = key,
    HL_S3_SECRET = secret,
    HL_S3_MAIN_FOLDER = main_folder,
    HL_S3_BUCKET = bucket,
    HL_S3_ENDPOINT = endpoint,
    HL_S3_REGION = region
  )

  message("Connexion S3 initialisée avec s3_connection_HL")
}



#' @title s3 create client
#'
#' @param key (chr) S3 key
#' @param secret (chr) s3 secret
#' @param main_folder (chr) folder added behind object
#' @param bucket (chr) s3 bucket
#' @param endpoint (chr) endpoint nécessaire pour accéder à S3
#' @param region (chr) s3 region, OVH == ""
#' @param use_config_files (log) TRUE if use with files config files
#'
#' @importFrom paws s3
#' @importFrom sodium hash
#'
#' @returns paws variable
#' @export
#'
#' @examples
#' if(interactive()){
#' s3_client_setup()
#' }
s3_client_setup <- function(key = NA,
                            secret = NA,
                            main_folder = NA,
                            bucket = NA,
                            endpoint = NA,
                            region = NA,
                            use_config_files = FALSE) {

  #  Lecture des fichiers de config si demandé

  if (use_config_files) {
    config_s3_access <- readRDS("inst/app/data/config_s3_access.rds")
    config_s3_location <- readRDS("inst/app/data/config_s3_location.rds")
    secure_key_raw <- sodium::hash(charToRaw(config_s3_location$secure_key))

    if (is.na(key))    key      <- rawToChar(sodium::data_decrypt(bin = config_s3_access$S3_ACCESS_KEY_ID_HL, key = secure_key_raw))
    if (is.na(secret)) secret   <- rawToChar(sodium::data_decrypt(bin = config_s3_access$S3_SECRET_ACCESS_KEY_HL, key = secure_key_raw))
    if (is.na(bucket)) bucket   <- config_s3_location$s3_bucket
    if (is.na(main_folder)) main_folder <- config_s3_location$s3_main_folder
    if (is.na(region)) region   <- config_s3_access$S3_region
    if (is.na(endpoint)) endpoint <- config_s3_access$s3_ENDPOINT

  }

  #  Vérification minimale
  if (any(is.na(c(key, secret, bucket, region, endpoint)))) {
    stop("S3 non initialisé correctement. Vérifiez vos fichiers de config ou les arguments passés.")
  }

  #  Création du client paws
  client <- paws::s3(
    credentials = list(
      creds = list(
        access_key_id = key,
        secret_access_key = secret
      )
    ),
    endpoint = endpoint,
    region = region
  )

  #  Ajouter les infos supplémentaires dans l'objet client
  client$bucket <- bucket
  client$main_folder <- main_folder

  message("Connexion S3 initialisée avec paws et informations stockées dans le client")
  return(client)
}
