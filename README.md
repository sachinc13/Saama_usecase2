# Saama_usecase2

In use case 2,I have prefromed below task.

1)Extracting zip file and loading it in to the temp location(refer function-unzip_file_fn() in usecase_2.py file)

2)Reading extracted gz files, converting it into csv and load it into landing location with specifed folder naming convention(/<<yearmonth>>/)(refer function-move_in_landing() in usecase_2.py file) 

3)creating 3 athena tables(which are pointing latest received files) on loaded files using crawler.(refer function-create_crawler() in usecase_2.py file) 

4)Archiving files from previous month, if recieved new files(refer function-move_old_files_arch() in usecase_2.py file) 
