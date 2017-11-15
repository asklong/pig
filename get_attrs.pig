SET mapred.map.tasks.speculative.execution true;
SET mapred.reduce.tasks.speculative.execution true;  
SET mapred.compress.map.output true;   
SET pig.tmpfilecompression true;       
SET pig.tmpfilecompression.codec lzo;  
SET default_parallel 2000;              
SET job.name '${job_name}';            

m03Lookup = LOAD 'my.item_data' 
            USING org.apache.hcatalog.pig.HCatLoader();
m03Lookup = FILTER m03Lookup BY dt== '${cur_date}';
                                  
m03Lookup = FOREACH m03Lookup 
            GENERATE (chararray)item_sku_id         AS sku_id,
                     sku_name,
                     item_name,
                     item_desc, 
                     barndname_full,
                     (chararray)item_first_cate_cd, 
                     item_first_cate_name,
                     (chararray)item_second_cate_cd,
                     item_second_cate_name,
                     (chararray)item_third_cate_cd,
                     item_third_cate_name, 
                     slogan, 
                     item_type;

m03Lookup = DISTINCT m03Lookup;

sku_query = LOAD '${sku_input}' 
            USING PigStorage() AS (sku_id:chararray);
                              
skuToCid = JOIN sku_query BY sku_id, 
                m03Lookup BY sku_id;

skuTocid = FOREACH skuToCid GENERATE 
                sku_query::sku_id                       AS sku_id,
                m03Lookup::sku_name                     AS sku_name,
                m03Lookup::item_name                    AS item_name,
                m03Lookup::item_desc                    AS item_desc,
                m03Lookup::barndname_full               AS barndname_full,
                m03Lookup::item_first_cate_name         AS item_first_cate_name,
                m03Lookup::item_second_cate_name        AS item_second_cate_name,
                m03Lookup::item_third_cate_name         AS item_third_cate_name,
                m03Lookup::slogan                       AS slogan,
                m03Lookup::item_type                    AS item_type;

skuToAttr = LOAD 'my.item_atrr_data'
            USING org.apache.hcatalog.pig.HCatLoader();
skuToAttr = FILTER skuToAttr BY dt == '${cur_date}';
skuToAttr = FOREACH skuToAttr 
            GENERATE (chararray)item_sku_id         AS sku_id, 
                CONCAT(ext_attr_name, '_', ext_attr_value_name)  AS sku_attr;
skuToAttr = DISTINCT skuToAttr;

skuToCidToAttr = JOIN skuTocid BY sku_id, skuToAttr BY sku_id;
skuToCidToAttr = FOREACH skuToCidToAttr GENERATE
                 skuTocid::sku_id                        AS sku_id,
                 skuTocid::sku_name                      AS sku_name,
                 skuTocid::item_name                     AS item_name,
                 skuTocid::item_desc                     AS item_desc,
                 skuTocid::barndname_full                AS barndname_full,
                 skuTocid::item_first_cate_name          AS item_first_cate_name,
                 skuTocid::item_second_cate_name         AS item_second_cate_name,
                 skuTocid::item_third_cate_name          AS item_third_cate_name,
                 skuTocid::slogan                        AS slogan,
                 skuTocid::item_type                     AS item_type,
                 skuToAttr::sku_attr                     AS sku_attr;


sku_main_image = LOAD 'my.image'
                 USING org.apache.hcatalog.pig.HCatLoader();

sku_main_image = FOREACH sku_main_image GENERATE
                         (chararray)skuid  AS sku_id, 
                         (chararray)image   AS image;

skuToCidToAttrToImage = JOIN skuToCidToAttr          BY sku_id,
                             sku_main_image          BY sku_id;
                               
skuToCidToAttrToImage = FOREACH skuToCidToAttrToImage GENERATE
          skuToCidToAttr::sku_id                AS sku_id,
          skuToCidToAttr::sku_name              AS sku_name,
          skuToCidToAttr::item_name             AS item_name,
          skuToCidToAttr::item_desc             AS item_desc,
          skuToCidToAttr::barndname_full        AS barndname_full,
          skuToCidToAttr::item_first_cate_name  AS item_first_cate_name,
          skuToCidToAttr::item_second_cate_name AS item_second_cate_name,
          skuToCidToAttr::item_third_cate_name  AS item_third_cate_name,
          skuToCidToAttr::slogan                AS slogan,
          skuToCidToAttr::item_type             AS item_type,
          skuToCidToAttr::sku_attr              AS sku_attr,
          sku_main_image::image                 AS image;


skuToCidToAttrToImage = FOREACH (GROUP skuToCidToAttrToImage  BY (sku_id, sku_name, item_name, item_desc, barndname_full, item_first_cate_name, item_second_cate_name, item_third_cate_name, slogan, item_type, image)) {
                         skuToCidToAttr_f = FOREACH skuToCidToAttrToImage
                                            GENERATE sku_attr AS sku_attr;
                 GENERATE flatten(group),
                          BagToString(skuToCidToAttr_f, '|')  AS sku_attrs; 
                 }

skuToCidToAttrToImage = FILTER skuToCidToAttrToImage BY (image IS NOT NULL                  AND 
                                                         image != ''                        AND
                                                         item_first_cate_name IS NOT NULL   AND
                                                         item_first_cate_name != ''         AND
                                                         item_second_cate_name IS NOT NULL  AND
                                                         item_second_cate_name != ''        AND
                                                         item_third_cate_name IS NOT NULL   AND
                                                         item_third_cate_name != '');
outputs = DISTINCT skuToCidToAttrToImage;


STORE outputs INTO '${sku_output}' parallel 1;
