
#' Fonction pour lire un .rds sur S3
#'
#' @param object Fichier à lire (avec sous dossier)
#' @param bucket Bucket S3
#' @param main_folder (log) Dossier principal dans "HL_S3_MAIN_FOLDER" ajouté devant object à lire
#' @param key Key pour accès au S3
#' @param secret Secret pour accès au S3
#' @param endpoint Enpoint pour accéder au S3
#' @param region Region (parfoit nécessaire), AWS est requis, OVH doit == ""
#'
#' @importFrom aws.s3 s3readRDS
#'
#' @returns Valeur du fichier .rds
#' @export
#'
#' @examples
#' if(interactive()){
#' s3readRDS_HL(object)
#' }
s3readRDS_HL <- function(object,
                         bucket = NA,
                         main_folder = TRUE,
                         key = NA,
                         secret = NA,
                         endpoint = NA,
                         region = NA) {



  #  Récupérer la valeur depuis la variable environnement si argument = NA
  if (is.na(key)) key <- Sys.getenv("HL_S3_KEY")
  if (is.na(secret)) secret <- Sys.getenv("HL_S3_SECRET")
  if (is.na(bucket)) bucket <- Sys.getenv("HL_S3_BUCKET")
  if (isTRUE(main_folder)) main_folder <- Sys.getenv("HL_S3_MAIN_FOLDER")
  if (is.na(endpoint)) endpoint <- Sys.getenv("HL_S3_ENDPOINT")
  if (is.na(region)) region <- Sys.getenv("HL_S3_REGION")

  #  Vérification minimale
  if (any(is.na(c(key, secret, bucket, endpoint, region)))) {
    stop("S3 non initialisé correctement. Vérifiez vos variables d'environnement ou les arguments passés.")
  }
  if (!is.character(main_folder)) {
    stop("Pour utiliser 'main_folder = TRUE', une valeur doit être écrit dans environment variable: 'HL_S3_MAIN_FOLDER'")
  }

  # si main_folder n'est pas NA, on l'ajoute devant l'objet
  if (!is.na(main_folder)) {
    object <- paste0(main_folder, "/", object)
  }

  #  Appel à aws.s3
  aws.s3::s3readRDS(
    object = object,
    bucket = bucket,
    key = key,
    secret = secret,
    region = region,
    base_url = endpoint
  )
}
