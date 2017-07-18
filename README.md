# fhbase
hdfs-sparkStream-hbase  Use the sparkstream to monitor files in the hdfs folder in real time and write them to hbase

I tested the compressed file and the text file

fhbase/data/my.tar.gz  and caca.txt   is working properly

but if your file type is *.tar.gz it will

scan 'hehe'
ROW                                   COLUMN+CELL                                                                                               
 2kk                                  column=f1:hp, timestamp=1500367115256, value=ll                                                           
 3kk                                  column=f1:hp, timestamp=1500367115256, value=ll                                                           
 4kk                                  column=f1:hp, timestamp=1500367115256, value=ll                                                           
 5kk                                  column=f1:hp, timestamp=1500367115256, value=ll                                                           
 6kk                                  column=f1:hp, timestamp=1500367115256, value=ll                                                           
 7hh                                  column=f1:hp, timestamp=1500367115256, value=pp                                                           
 8mm                                  column=f1:hp, timestamp=1500367115256, value=nn                                                           
 caca\x00\x00\x00\x00\x00\x00\x00\x00 column=f1:hp, timestamp=1500367115256, value=ll                                                           
 \x00\x00\x00\x00\x00\x00\x00\x00\x00                                                                                                           
 \x00\x00\x00\x00\x00\x00\x00\x00\x00                                                                                                           
 \x00\x00\x00\x00\x00\x00\x00\x00\x00                                                                                                           
 \x00\x00\x00\x00\x00\x00\x00\x00\x00                                                                                                           
 \x00\x00\x00\x00\x00\x00\x00\x00\x00                                                                                                           
 \x00\x00\x00\x00\x00\x00\x00\x00\x00                                                                                                           
 \x00\x00\x00\x00\x00\x00\x00\x00\x00                                                                                                           
 \x00\x00\x00\x00\x00\x00\x00\x00\x00                                                                                                           
 \x00\x00\x00\x00\x00\x00\x00\x00\x00                                                                                                           
 \x00\x00\x00\x00\x00\x00\x000000644\                                                                                                           
 x000001750\x000001750\x0000000000104                                                                                                           
 \x0013133344145\x00010650\x00 0\x00\         
//** 

you can control filter at next
  println("==================>the key is " + p(0) + "  the value is ==>>  " + p(1))
      if (p(0).length == 2 ) {  
        println("==================>" + p(0) + "  <<=== 风骚的分割线  ==>>  " + p(1))
        
        **/
 
 《《step1>  create 'hehe','f1'  //create hbase table
 
