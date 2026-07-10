#!/bin/bash

function rm_file()
{
    target_file=$1
    if [[ -f ${target_file} ]]; then
        rm -rf ${target_file}
    fi
}

# eg: copy file catalog.hive.properties to dest_dir/catalog/hive.properties
function dep_cp_file()
{
    src_file=$1
    dest_dir=$2

    if [[ -f ${src_file} ]] && [[ -d ${dest_dir} ]]; then
        src_dir=`dirname ${src_file}`
        if [[ "${src_dir}" == "${dest_dir}" ]]; then
            return 0
        fi

        src_filename=`basename ${src_file}`
        src_name_depth=`grep -o "\." <<< ${src_filename} |wc -l`

        if [ ${src_name_depth} -gt 1 ]; then
            sub_dir=`echo ${src_filename%%.*}`
            sub_filename=`echo ${src_filename#*.}`
            if [[ ! -d ${dest_dir}/${sub_dir} ]]; then
                mkdir -p ${dest_dir}/${sub_dir}
            fi
            
            rm_file ${dest_dir}/${sub_dir}/${sub_filename}
            cp ${src_file} ${dest_dir}/${sub_dir}/${sub_filename}
            sed -i 's/\\//g' ${dest_dir}/${sub_dir}/${sub_filename}
            chmod 644 ${dest_dir}/${sub_dir}/${sub_filename}
        else
            rm_file ${dest_dir}/${src_filename}
            cp ${src_file} ${dest_dir}
            sed -i 's/\\//g' ${dest_dir}/${src_filename}
            chmod 644 ${dest_dir}/${src_filename}
        fi
    fi
}

function dep_cp_dir()
{
    src_dir=$1
    dest_dir=$2
    if [[ -d ${src_dir} ]] && [[ -d ${dest_dir} ]]; then
        sub_files=$(ls ${src_dir})
        for filename in ${sub_files}
        do
            dep_cp_file ${src_dir}/${filename} ${dest_dir}
        done
    fi
}
