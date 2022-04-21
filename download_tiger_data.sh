#!/bin/bash

source .env

CURRENT_DIR="$(pwd)"
GISDATA="${CURRENT_DIR}/gisdata"
TMPDIR="${GISDATA}/temp/"
URLDIR="${GISDATA}/urls/"
YEAR=$GEOCODER_YEAR
BASEPATH="www2.census.gov/geo/tiger/TIGER${YEAR}"
BASEURL="https://${BASEPATH}"

mkdir -p "${TMPDIR}"
mkdir -p "${URLDIR}"
echo "Downloading ${YEAR} Census shapefile data for ${GEOCODER_STATES} to ${GISDATA}"

VERBOSE=0
while getopts 'v' flag; do
  case "${flag}" in
    v) VERBOSE=1 ;;
  esac
done
readonly VERBOSE


verbose_echo () {
  local msg=$1
  if [[ VERBOSE -eq 1 ]]; then
    echo "${msg}"
  fi
}

get_fips_from_abbr () {
  local abbr=$1
  local fips=0
  case $abbr in
    "AL")  fips=01;; "AK")  fips=02;; "AS")  fips=60;; "AZ")  fips=04;; "AR")  fips=05;;
    "CA")  fips=06;; "CO")  fips=08;; "CT")  fips=09;; "DE")  fips=10;; "DC")  fips=11;;
    "FL")  fips=12;; "FM")  fips=64;; "GA")  fips=13;; "GU")  fips=66;; "HI")  fips=15;;
    "ID")  fips=16;; "IL")  fips=17;; "IN")  fips=18;; "IA")  fips=19;; "KS")  fips=20;;
    "KY")  fips=21;; "LA")  fips=22;; "ME")  fips=23;; "MH")  fips=68;; "MD")  fips=24;;
    "MA")  fips=25;; "MI")  fips=26;; "MN")  fips=27;; "MS")  fips=28;; "MO")  fips=29;;
    "MT")  fips=30;; "NE")  fips=31;; "NV")  fips=32;; "NH")  fips=33;; "NJ")  fips=34;;
    "NM")  fips=35;; "NY")  fips=36;; "NC")  fips=37;; "ND")  fips=38;; "MP")  fips=69;;
    "OH")  fips=39;; "OK")  fips=40;; "OR")  fips=41;; "PW")  fips=70;; "PA")  fips=42;;
    "PR")  fips=72;; "RI")  fips=44;; "SC")  fips=45;; "SD")  fips=46;; "TN")  fips=47;;
    "TX")  fips=48;; "UM")  fips=74;; "UT")  fips=49;; "VT")  fips=50;; "VA")  fips=51;;
    "VI")  fips=78;; "WA")  fips=53;; "WV")  fips=54;; "WI")  fips=55;; "WY")  fips=56;;
  esac
  echo "${fips}"
}

get_fips_files () {
  local url=$1
  local fips=$2
  local files=($(wget --no-verbose -O - "${url}" \
    | perl -nle 'print if m{(?=\"tl)(.*?)(?<=>)}g' \
    | perl -nle 'print m{(?=\"tl)(.*?)(?<=>)}g' \
    | sed -e 's/[\">]//g'))
  local matched=($(echo "${files[*]}" | tr ' ' '\n' | grep "tl_${YEAR}_${fips}"))
  echo "${matched[*]}"
}

format_tabblock_variables() {
  tabblock10_file_label="tabblock10"
  tabblock_geoid_colname="geoid10"
  if [[ "${YEAR}" -ge 2011 && "${YEAR}" -lt 2014 ]]; then
    tabblock10_file_label="tabblock"
    tabblock_geoid_colname="geoid"
  # elif [[ "${YEAR}" -ge 2020 ]]; then
  #   tabblock_geoid_colname="geoid20"
  fi
}

make_arrays_of_file_urls() {
  download_urls+=("${BASEURL}/STATE/tl_${YEAR}_us_state.zip")
  download_urls+=("${BASEURL}/COUNTY/tl_${YEAR}_us_county.zip")

  # State data
  if [ "${GEOCODER_STATES}" = '*' ]; then
    echo "'*' detected for STATES parameter. Adding data for all US states..."
    GEOCODER_STATES="AL,AK,AZ,AR,CA,CO,CT,DE,FL,GA,HI,ID,IL,IN,IA,KS,KY,LA,ME,MD,MA,MI,MN,MS,MO,MT,NE,NV,NH,NJ,NM,NY,NC,ND,OH,OK,OR,PA,RI,SC,SD,TN,TX,UT,VT,VA,WA,WV,WI,WY"
  fi

  # For each selected state
  IFS=',' read -ra STATES <<< "${GEOCODER_STATES}"
  for i in "${STATES[@]}"; do
    ABBR="${i}"
    FIPS=$(get_fips_from_abbr "${ABBR}")
    if [ "${FIPS}" -eq 0 ]; then
      echo "Error: '${ABBR}' is not a recognized US state abbreviation"
    else
      # echo '----------------------------------------'
      # echo "      Loading state data for: '$ABBR $FIPS'"
      # echo '----------------------------------------'
      download_urls+=("${BASEURL}/PLACE/tl_${YEAR}_${FIPS}_place.zip")
      download_urls+=("${BASEURL}/COUSUB/tl_${YEAR}_${FIPS}_cousub.zip")
      download_urls+=("${BASEURL}/TRACT/tl_${YEAR}_${FIPS}_tract.zip")

      download_urls+=("${BASEURL}/TABBLOCK/tl_${YEAR}_${FIPS}_${tabblock10_file_label}.zip")
      download_urls+=("${BASEURL}/BG/tl_${YEAR}_${FIPS}_bg.zip")
      # if [[ "${YEAR}" -ge 2020 ]]; then
      #   download_urls+=("${BASEURL}/TABBLOCK20/tl_${YEAR}_${FIPS}_tabblock20.zip")
      # fi

      no_reject_urls+=("${BASEURL}/FACES ${FIPS}")

      URL_FILE="${URLDIR}/${ABBR}_faces_urls.txt"
      if [ -f "${URL_FILE}" ]; then
        verbose_echo "${URL_FILE} already pulled"
      else
        files=($(get_fips_files "${BASEURL}/FACES" "${FIPS}"))
        for i in "${files[@]}"; do
          echo "${BASEURL}/FACES/$i" >> "${URL_FILE}"
          no_reject_urls+=("${BASEURL}/FACES/${i}")
          verbose_echo "${BASEURL}/FACES/${i}"
        done
      fi

      URL_FILE="${URLDIR}/${ABBR}_featnames_urls.txt"
      if [ -f "${URL_FILE}" ]; then
        verbose_echo "${URL_FILE} already pulled"
      else
        files=($(get_fips_files "${BASEURL}/FEATNAMES" "${FIPS}"))
        for i in "${files[@]}"; do
          echo "${BASEURL}/FEATNAMES/${i}" >> "${URL_FILE}"
          no_reject_urls+=("${BASEURL}/FEATNAMES/${i}")
          verbose_echo "${BASEURL}/FEATNAMES/${i}"
        done
      fi

      URL_FILE="${URLDIR}/${ABBR}_edges_urls.txt"
      if [ -f "${URL_FILE}" ]; then
        verbose_echo "${URL_FILE} already pulled"
      else
        files=($(get_fips_files "${BASEURL}/EDGES" "${FIPS}"))
        for i in "${files[@]}"; do
          echo "${BASEURL}/EDGES/${i}" >> "${URL_FILE}"
          no_reject_urls+=("${BASEURL}/EDGES/${i}")
          verbose_echo "${BASEURL}/EDGES/${i}"
        done
      fi

      URL_FILE="${URLDIR}/${ABBR}_addr_urls.txt"
      if [ -f "${URL_FILE}" ]; then
        verbose_echo "${URL_FILE} already pulled"
      else
        files=($(get_fips_files "${BASEURL}/ADDR" "${FIPS}"))
        for i in "${files[@]}"; do
          echo "${BASEURL}/ADDR/${i}" >> "${URL_FILE}"
          no_reject_urls+=("${BASEURL}/ADDR/${i}")
          verbose_echo "${BASEURL}/ADDR/${i}"
        done
      fi
    fi
  done
}

download_national_and_statewide_files() {
  local downloaded_files=()
  for download_url in "${download_urls[@]}"; do
    OUTPUT_FILE="${GISDATA}/${download_url}"
    if [ -f "${OUTPUT_FILE}" ]; then
      verbose_echo "${OUTPUT_FILE} already pulled"
      downloaded_files+=("${download_url}")
    else
      verbose_echo "downloading ${file_url} to location ${OUTPUT_FILE}"
      OUTPUT_DIR=$(dirname $OUTPUT_FILE)
      verbose_echo "${OUTPUT_DIR}"
      mkdir -p "${OUTPUT_DIR}"
      wget -O "${OUTPUT_FILE}" "${download_url}" --mirror --reject=html
    fi
  done
  undownloaded_files=(`echo ${download_urls[@]} ${downloaded_files[@]} | tr ' ' '\n' | sort | uniq -u`)
  n_undownloaded=$(echo "${#undownloaded_files[@]}")
  if [ "${n_undownloaded}" -gt 0 ]; then
    echo "Difference in file_urls and previously downloaded_files"
    echo "Number of undownloaded_files: ${n_undownloaded}"
    echo "${undownloaded_files}"
    echo "Run this shell script again (after redirecting your VPN)."
    missing_files+=("${undownloaded_files}")
  # else
  #   echo "All national and statewide files successfully downloaded."
  fi
}

download_files_from_url_file () {
  local file_path=$1
  readarray -t file_urls < $1
  local downloaded_files=()
  for file_url in "${file_urls[@]}"; do
    OUTPUT_FILE="${GISDATA}/${file_url}"
    if [ -f "${OUTPUT_FILE}" ]; then
      verbose_echo "${OUTPUT_FILE} already pulled"
      downloaded_files+=("${file_url}")
    else
      verbose_echo "downloading ${file_url} to location ${OUTPUT_FILE}"
      OUTPUT_DIR=$(dirname "${OUTPUT_FILE}")
      mkdir -p "${OUTPUT_DIR}"
      wget -O "${OUTPUT_FILE}" "${file_url}" --mirror
    fi
  done
  undownloaded_files=(`echo ${file_urls[@]} ${downloaded_files[@]} | tr ' ' '\n' | sort | uniq -u`)
  n_undownloaded=$(echo "${#undownloaded_files[@]}")
  if [ "${n_undownloaded}" -gt 0 ]; then
    echo "Difference in file_urls and previously downloaded_files"
    echo "Number of undownloaded_files: ${n_undownloaded}"
    echo "${undownloaded_files}"
    echo "Run this shell script again (after redirecting your VPN)."
    missing_files+=("${undownloaded_files}")
  # else
    # echo "All files from ${file_path} successfully downloaded."
  fi
}

create_array_of_url_files() {
  local -n url_file_paths=$1
  for url_file_path in "${URLDIR}"/*; do
    url_file_paths+=("${url_file_path}")
  done
}

download_data_from_urls_in_url_files() {
  local file_paths
  create_array_of_url_files file_paths
  for file_path in "${file_paths[@]}"; do
    verbose_echo "${file_path}"
    download_files_from_url_file "${file_path}"
  done
}

check_for_empty_files() {
  local file_paths
  create_array_of_url_files file_paths
  for file_path in "${file_paths[@]}"; do
    verbose_echo "${file_path}"
    readarray -t file_urls < "${file_path}"
    local downloaded_files=()
    for file_url in "${file_urls[@]}"; do
      OUTPUT_FILE="${GISDATA}/${file_url}"
      if [ -s "${OUTPUT_FILE}" ]; then
        continue
      else
        echo "zero-byte file: $(du -sh ${OUTPUT_FILE})"
        zero_byte_files+=("${OUTPUT_FILE}")
      fi
    done
  done
  for download_url in "${download_urls[@]}"; do
    OUTPUT_FILE="${GISDATA}/${download_url}"
    if [ -s "${OUTPUT_FILE}" ]; then
      continue
    else
      echo "zero-byte file: $(du -sh ${OUTPUT_FILE})"
      zero_byte_files+=("${OUTPUT_FILE}")
    fi
  done
}

remove_zero_byte_files() {
  if [ "${#zero_byte_files[@]}" -gt 0 ]; then
    for zero_byte_file in "${zero_byte_files[@]}"; do
      echo "Removing zero-byte file: ${zero_byte_file}"
      rm "${zero_byte_file}"
    done
    echo "Rerun this script to retry downloading files"
  fi
}

check_that_all_files_downloaded() {
  if [ "${#missing_files[@]}" -eq 0 ]; then
    echo "All files downloaded."
  else
    for missing_file in "${missing_files[@]}"; do
      echo "File missing: ${missing_file}"
    done
    echo "Rerun this script to retry downloading files"
  fi
}

declare -a zero_byte_files
declare -a missing_files

format_tabblock_variables
make_arrays_of_file_urls
download_national_and_statewide_files
download_data_from_urls_in_url_files
check_for_empty_files
check_that_all_files_downloaded
remove_zero_byte_files
