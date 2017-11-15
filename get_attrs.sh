source $HOME/.bash_profile

function get_attrs {
    # set parameters
    project_hdfs_path="/item_ids"
    pig_script_sku="/get_attrs.pig"
    day=`date -d "0 days ago" +%Y-%m-%d`
    sku_input=$project_hdfs_path"/${day}"
    sku_output="/item_infor/${day}"
 

    hadoop fs -test -e $sku_output
    if [ $? -eq 0 ]
    then
        hadoop fs -rmr $sku_output
    fi


    # run script
    start_date=`date -d "7 days ago" +%Y-%m-%d`
    cur_date=`date -d "2 days ago" +%Y-%m-%d`
    job_name="join_sku_attributes_"$cur_date
    echo "current date is: "$cur_date
    echo "job name is: "$job_name
    echo "input is: "$sku_input
    echo "output is: "$sku_output

    pig -useHCatalog -p job_name=$job_name \
                     -p start_date=$start_date \
                     -p sku_input=$sku_input \
                     -p sku_output=$sku_output \
                     -p cur_date=$cur_date \
                     $pig_script_sku
}
