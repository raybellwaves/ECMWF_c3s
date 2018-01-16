#!/bin/ksh
#
# /projects/rsmas/kirtman/rxb826/DATA/MetOffice_GloSea5/post_proc.ksh
#
# Split ensembles and post process data doawnloading in get_data.ksh
# 1/14/18
# Run as bsub < get_data_submit.sh

topdir=`pwd`
mkdir -p batch_files2 logs2
rm -rf batch_files2/*
rm -rf logs2/*

# Get file settings
varname=mslp

#initialconditionarr=(10 11 12)
#startdayarr=(01 09 17 25)
initialconditionarr=(10)
startdayarr=(01)

regionname=DJF

for i in {1..28}; do
    mkdir -p ${varname}/6hourly/ens${i}
    mkdir -p ${varname}/daily/ens${i}
    mkdir -p ${varname}/monthly/ens${i}
    mkdir -p ${varname}/seasonal/ens${i}
done

if [ ! -f batch_files2/mygrid_1deg ];then
cat > batch_files2/mygrid_1deg << EOF
gridtype = lonlat
xsize    = 360
ysize    = 181
xfirst   = 0.0
xinc     = 1
yfirst   = -90.0
yinc     = 1
EOF
fi

# Calculate loop sizes
nic=${#initialconditionarr[@]}
nsd=${#startdayarr[@]}
let nic=$nic-1
let nsd=$nsd-1
fileref=0

for i in {0..${nic}}; do
    initialcondition=${initialconditionarr[$i]}
    for j in {0..${nsd}}; do
        startday=${startdayarr[$j]}
            if [[ ${startday} == 01 ]]; then
                ensarr={1..7}
            elif [[ ${startday} == 09 ]]; then
                ensarr={8..14}
            elif [[ ${startday} == 17 ]]; then
                ensarr={15..21}
            else
                ensarr={22..28}
            fi            
            for year in {1994..2010}; do
                year2=`expr $year + 1`
                enscounter=1
                for ens in ${ensarr}; do
                    echo "#!/bin/ksh" > batch_files2/file_${fileref}.ksh
                    chmod u+x batch_files2/file_${fileref}.ksh

                    # Extrack ensemble
                    echo "ncks -O -d number,${enscounter} ${varname}/6hourly/rawdata/${year}${initialcondition}${startday}_7ens_${regionname}.nc ${varname}/6hourly/ens${ens}/${year}${initialcondition}${startday}_${regionname}.nc" >> batch_files2/file_${fileref}.ksh
                    # Remove dimension
                    echo "ncwa -O -a number ${varname}/6hourly/ens${ens}/${year}${initialcondition}${startday}_${regionname}.nc ${varname}/6hourly/ens${ens}/${year}${initialcondition}${startday}_${regionname}.nc" >> batch_files2/file_${fileref}.ksh
                    echo "ncks -O -x -v number ${varname}/6hourly/ens${ens}/${year}${initialcondition}${startday}_${regionname}.nc ${varname}/6hourly/ens${ens}/${year}${initialcondition}${startday}_${regionname}.nc" >> batch_files2/file_${fileref}.ksh
                    # Reverse latitude?

                    # Convert to 360 x 181 (test it can overwrite file otherwise move it)
                    echo "cdo remapbil,batch_files2/mygrid_1deg ${varname}/6hourly/ens${ens}/${year}${initialcondition}${startday}_${regionname}.nc ${varname}/6hourly/ens${ens}/${year}${initialcondition}${startday}_${regionname}.nc.tmp" >> batch_files2/file_${fileref}.ksh
                    echo "ncrename -O -d lon,longitude -d lat,latitude -v lon,longitude -v lat,latitude ${varname}/6hourly/ens${ens}/${year}${initialcondition}${startday}_${regionname}.nc.tmp ${varname}/6hourly/ens${ens}/${year}${initialcondition}${startday}_${regionname}.nc" >> batch_files2/file_${fileref}.ksh

                    # Variable specifications
                    if [[ ${varname} == mslp ]];then
                        echo "ncap2 -O -s 'msl=msl/100' ${varname}/6hourly/ens${ens}/${year}${initialcondition}${startday}_${regionname}.nc ${varname}/6hourly/ens${ens}/${year}${initialcondition}${startday}_${regionname}.nc" >> batch_files2/file_${fileref}.ksh
                        echo 'ncatted -O -a units,msl,o,c,"hPa" '${varname}'/6hourly/ens'${ens}/${year}${initialcondition}${startday}_${regionname}'.nc' >> batch_files2/file_${fileref}.ksh
                        echo "ncrename -O -v msl,${varname} ${varname}/6hourly/ens${ens}/${year}${initialcondition}${startday}_${regionname}.nc ${varname}/6hourly/ens${ens}/${year}${initialcondition}${startday}_${regionname}.nc" >> batch_files2/file_${fileref}.ksh
                    fi

                    # Average to seasonal. Eventually do properly with pandas 
                    # Match file name for paper
                    echo "ncra -O ${varname}/6hourly/ens${ens}/${year}${initialcondition}${startday}_${regionname}.nc ${varname}/seasonal/ens${ens}/GloSea5_ens${ens}_${initialcondition}ic_${year}-${year2}_DJFmean_global.nc" >> batch_files2/file_${fileref}.ksh
                    # Add ensemble in
                    echo "ncap2 -s 'defdim("'"'ensemble'"'",1);ensemble[ensemble]="${ens}";ensemble@long_name="'"ensemble"'"'"" -O ${varname}/seasonal/ens${ens}/GloSea5_ens${ens}_${initialcondition}ic_${year}-${year2}_DJFmean_global.nc ${varname}/seasonal/ens${ens}/GloSea5_ens${ens}_${initialcondition}ic_${year}-${year2}_DJFmean_global.nc.tmp" >> batch_files2/file_${fileref}.ksh
                    echo "mv ${varname}/seasonal/ens${ens}/GloSea5_ens${ens}_${initialcondition}ic_${year}-${year2}_DJFmean_global.nc.tmp ${varname}/seasonal/ens${ens}/GloSea5_ens${ens}_${initialcondition}ic_${year}-${year2}_DJFmean_global.nc" >> batch_files2/file_${fileref}.ksh

                    # Submit script
                    echo "#BSUB -o logs2/file_${fileref}.out" > batch_files2/file_${fileref}_submit.sh
                    echo "#BSUB -e logs2/file_${fileref}.err" >> batch_files2/file_${fileref}_submit.sh
                    echo "#BSUB -W 0:05" >> batch_files2/file_${fileref}_submit.sh
                    echo "#BSUB -q general" >> batch_files2/file_${fileref}_submit.sh
                    echo "#BSUB -n 1" >> batch_files2/file_${fileref}_submit.sh
                    echo "#" >> batch_files2/file_${fileref}_submit.sh
                    echo "batch_files2/file_${fileref}.ksh" >> batch_files2/file_${fileref}_submit.sh

                    # Check that the file hasn't been created
                    if [[ ! -f ${varname}/seasonal/ens${ens}/GloSea5_ens${ens}_${initialcondition}ic_${year}-${year2}_DJFmean_global.nc ]]; then
                        echo "creating file ${varname}/seasonal/ens${ens}/GloSea5_ens${ens}_${initialcondition}ic_${year}-${year2}_DJFmean_global.nc"
                        bsub < batch_files2/file_${fileref}_submit.sh
                        let fileref=$fileref+1
                    else
                        echo "file ${varname}/seasonal/ens${ens}/GloSea5_ens${ens}_${initialcondition}ic_${year}-${year2}_DJFmean_global.nc exists"
                    fi

            # Loop over ensemble
            let enscounter=$enscounter+1
            done
        # Loop over year
        exit 0
        done
    # Loop over start daty
    done
# Loop over initial conditions
done
