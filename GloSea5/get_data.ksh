#!/bin/ksh
#
# /projects/rsmas/kirtman/rxb826/DATA/MetOffice_GloSea5/get_data.ksh
#
# 7 ensembles for 4 start dates (01 09 17 25)
# Submit a maximum of 20 jobs
# 1/11/18
# Run as bsub < get_data_submit.sh

topdir=`pwd`
mkdir -p batch_files
mkdir -p logs
rm -rf batch_files/*
rm -rf logs/*

# Get file settings
varname=mslp

initialcondition="10" # 01 .. 12
startday="09" # 01, 09, 17, 25

extractregion=1
if [[ ${extractregion} -eq 1 ]];then
    # Add lon-lat in later

    times=1201
    timee=0228
    regionname=DJF

    # Convert time to numbers to check if region crosses 1231
    typeset -i timesnum
    timesnum=${times%.*}
    typeset -i timeenum
    timeenum=${timee%.*}
    if [[ ${timeenum} -lt ${timesnum} ]];then
        regionalcrossyear=1
    else
        regionalcrossyear=0
    fi
fi

mkdir -p ${varname}
mkdir -p ${varname}/6hourly
mkdir -p ${varname}/6hourly/rawdata
outdir=${topdir}/${varname}/6hourly/rawdata/

# Convert to ECMWF server request
if [[ ${varname} == mslp ]];then
    param=151.128
fi

# Calculate steps work out for one year
# Output value of python pandas script:x
if [[ ${initialcondition} == "10" ]];then
    if [[ ${startday} == "01" ]];then
        startstep=1464
        endstep=3618
    fi
    if [[ ${startday} == "09" ]];then
        startstep=1272
        endstep=3426
    fi
    if [[ ${startday} == "17" ]];then
        startstep=1080
        endstep=3334
    fi
    if [[ ${startday} == "25" ]];then
        startstep=888
        endstep=3042
    fi
fi
if [[ ${initialcondition} == "11" ]];then
    if [[ ${startday} == "01" ]];then
        startstep=720
        endstep=2874
    fi
    if [[ ${startday} == "09" ]];then
        startstep=528
        endstep=2682
    fi
    if [[ ${startday} == "17" ]];then
        startstep=336
        endstep=2490
    fi
    if [[ ${startday} == "25" ]];then
        startstep=144
        endstep=2298
    fi
fi
if [[ ${initialcondition} == "12" ]];then
    if [[ ${startday} == "01" ]];then
        startstep=0
        endstep=2156
    fi
    if [[ ${startday} == "09" ]];then
        startstep=0
        endstep=1960
    fi
    if [[ ${startday} == "17" ]];then
        startstep=0
        endstep=1770
    fi
    if [[ ${startday} == "25" ]];then
        startstep=0
        endstep=1578
    fi
fi

i=0
for year in {1994..2010}; do
    for sd in ${startday}; do
        echo "#!/bin/ksh" > batch_files/f_${i}.ksh
        chmod u+x batch_files/f_${i}.ksh
cat > batch_files/f_${i}.py << EOF
from ecmwfapi import ECMWFDataServer
server = ECMWFDataServer()

server.retrieve({
    "class": "c3",
    "dataset": "c3s_seasonal",
    "date": "${year}-${initialcondition}-${startday}",
    "expver": "1",
    "levtype": "sfc",
    "method": "1",
    "number": "101/102/103/104/105/106/107",
    "origin": "egrr",
    "param": "${param}",
    "step": "${startstep}/to/${endstep}/by/6",
    "stream": "mmsf",
    "system": "12",
    "time": "00:00:00",
    "type": "fc",
    "format": "grib",
    "target": "${outdir}${year}${initialcondition}${startday}_7ens_DJF.grib",
})
EOF
        echo "python batch_files/f_${i}.py" >> batch_files/f_${i}.ksh
        echo "grib_to_netcdf -R 18500101 -o ${outdir}${year}${initialcondition}${startday}_7ens_DJF.nc ${outdir}${year}${initialcondition}${startday}_7ens_DJF.grib" >> batch_files/f_${i}.ksh
        echo "rm -rf ${outdir}${year}${initialcondition}${startday}_7ens_DJF.grib" >> batch_files/f_${i}.ksh

        # Submit script
        echo "#BSUB -J f_${i}.ksh" >> batch_files/f_${i}_submit.sh
        echo "#BSUB -o logs/f_${i}.out" >> batch_files/f_${i}_submit.sh
        echo "#BSUB -e logs/f_${i}.err" >> batch_files/f_${i}_submit.sh
        echo "#BSUB -W 24:00" >> batch_files/f_${i}_submit.sh
        echo "#BSUB -q general" >> batch_files/f_${i}_submit.sh
        echo "#BSUB -n 1" >> batch_files/f_${i}_submit.sh
        echo "#" >> batch_files/f_${i}_submit.sh
        echo "batch_files/f_${i}.ksh" >> batch_files/f_${i}_submit.sh

        if [[ ! -f ${outdir}/${year}${initialcondition}${startday}_7ens_DJF.nc ]];then
            bsub < batch_files/f_${i}_submit.sh
            let i=$i+1
        fi
    done
done
