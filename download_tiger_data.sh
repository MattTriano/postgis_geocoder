#!/bin/bash

source .env

CURRENT_DIR="$(pwd)"
GISDATA="${CURRENT_DIR}/gisdata"
TMPDIR="${GISDATA}/temp/"
URLDIR="${GISDATA}/urls/"
YEAR=$GEOCODER_YEAR
BASEPATH="www2.census.gov/geo/tiger/TIGER${YEAR}"
BASEURL="https://${BASEPATH}"

mkdir -p ${TMPDIR}
mkdir -p ${URLDIR}
echo "${GISDATA}"
echo "${YEAR}"
echo "${GEOCODER_STATES}"

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
    echo $fips
}

get_fips_files () {
    local url=$1
    local fips=$2
    local files=($(wget --no-verbose -O - $url \
        | perl -nle 'print if m{(?=\"tl)(.*?)(?<=>)}g' \
        | perl -nle 'print m{(?=\"tl)(.*?)(?<=>)}g' \
        | sed -e 's/[\">]//g'))
    local matched=($(echo "${files[*]}" | tr ' ' '\n' | grep "tl_${YEAR}_${fips}"))
    echo "${matched[*]}"
}

download_urls+=(${BASEURL}/STATE/tl_${YEAR}_us_state.zip)
download_urls+=(${BASEURL}/COUNTY/tl_${YEAR}_us_county.zip)

# State data
if [ "$GEOCODER_STATES" = '*' ]; then
    echo "'*' detected for STATES parameter. Adding data for all US states..."
    GEOCODER_STATES="AL,AK,AZ,AR,CA,CO,CT,DE,FL,GA,HI,ID,IL,IN,IA,KS,KY,LA,ME,MD,MA,MI,MN,MS,MO,MT,NE,NV,NH,NJ,NM,NY,NC,ND,OH,OK,OR,PA,RI,SC,SD,TN,TX,UT,VT,VA,WA,WV,WI,WY"
    fi

# For each selected state
IFS=',' read -ra STATES <<< "$GEOCODER_STATES"
for i in "${STATES[@]}"; 
do
    ABBR=$i
    FIPS=$(get_fips_from_abbr $ABBR)
    if [ $FIPS -eq 0 ]; then
        echo "Error: '$ABBR' is not a recognized US state abbreviation"
    else
        # echo '----------------------------------------'
        # echo "      Loading state data for: '$ABBR $FIPS'"
        # echo '----------------------------------------'
        download_urls+=($BASEURL/PLACE/tl_${YEAR}_${FIPS}_place.zip)
        download_urls+=($BASEURL/COUSUB/tl_${YEAR}_${FIPS}_cousub.zip)
        download_urls+=($BASEURL/TRACT/tl_${YEAR}_${FIPS}_tract.zip)

        no_reject_urls+=($BASEURL/FACES $FIPS)

        URL_FILE="${URLDIR}/${ABBR}_faces_urls.txt"
        if [ -f "$URL_FILE" ]; then
            echo "$URL_FILE already pulled"
        else
            files=($(get_fips_files $BASEURL/FACES $FIPS))
            for i in "${files[@]}"
            do
                echo "$BASEURL/FACES/$i" >> "$URL_FILE"
                no_reject_urls+=($BASEURL/FACES/$i)
                echo $BASEURL/FACES/$i
            done
        fi

        URL_FILE="${URLDIR}/${ABBR}_featnames_urls.txt"
        if [ -f "$URL_FILE" ]; then
            echo "$URL_FILE already pulled"
        else
            files=($(get_fips_files $BASEURL/FEATNAMES $FIPS))
            for i in "${files[@]}"
            do
                echo "$BASEURL/FEATNAMES/$i" >> "$URL_FILE"
                no_reject_urls+=($BASEURL/FEATNAMES/$i)
                echo $BASEURL/FEATNAMES/$i
            done
        fi

        URL_FILE="${URLDIR}/${ABBR}_edges_urls.txt"
        if [ -f "$URL_FILE" ]; then
            echo "$URL_FILE already pulled"
        else
            files=($(get_fips_files $BASEURL/EDGES $FIPS))
            for i in "${files[@]}"
            do
                echo "$BASEURL/EDGES/$i" >> "$URL_FILE"
                no_reject_urls+=($BASEURL/EDGES/$i)
                echo $BASEURL/EDGES/$i
            done
        fi

        URL_FILE="${URLDIR}/${ABBR}_addr_urls.txt"
        if [ -f "$URL_FILE" ]; then
            echo "$URL_FILE already pulled"
        else
            files=($(get_fips_files $BASEURL/ADDR $FIPS))
            for i in "${files[@]}"
            do
                echo "$BASEURL/ADDR/$i" >> "$URL_FILE"
                no_reject_urls+=($BASEURL/ADDR/$i)
                echo $BASEURL/ADDR/$i
            done
        fi

    fi
done

for i in "${no_reject_urls[@]}"; do
  echo $i \
        | perl -nle 'print if m{(?=\"tl)(.*?)(?<=>)}g' \
        | perl -nle 'print m{(?=\"tl)(.*?)(?<=>)}g' \
        | sed -e 's/[\">]//g'
done

download_files_from_url_file () {
    local file_path=$1
    readarray -t file_urls < $1
    local downloaded_files=()
    for file_url in "${file_urls[@]}"; do
        OUTPUT_FILE="${GISDATA}/${file_url}"
        if [ -f "$OUTPUT_FILE" ]; then
            echo "$OUTPUT_FILE already pulled"
            downloaded_files+=($file_url)
        else
            # echo "downloading $file_url to location $OUTPUT_FILE"
            OUTPUT_DIR=$(dirname $OUTPUT_FILE)
            # echo $OUTPUT_DIR
            mkdir -p ${OUTPUT_DIR}
            wget -O $OUTPUT_FILE $file_url --mirror
        fi
        # echo $file_url
    done
    echo "Difference in fils_urls and previously downloaded_files"
    undownloaded_files=(`echo ${file_urls[@]} ${downloaded_files[@]} | tr ' ' '\n' | sort | uniq -u`)
    echo $undownloaded_files
}

echo "${URLDIR}"
echo "${URLDIR}AL_featnames_urls.txt"
download_files_from_url_file ${URLDIR}AL_featnames_urls.txt
