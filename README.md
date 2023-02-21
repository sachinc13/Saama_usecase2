# Saama_usecase2

In use case 2,I have prefromed below task.
1)Extracting zip file and loading it in to the temp location(refer function-unzip_file_fn() in usecase_2.py file)
2)Reading extracted gz files, converting it into csv and load it into landing location with specifed folder naming convention(/<<yearmonth>>/)(refer function-move_in_landing() in usecase_2.py file) 
3)creating 3 athena tables(which are pointing latest received files) on loaded files using crawler.(refer function-create_crawler() in usecase_2.py file) 
4)Archiving files from previous month, if recieved new files(refer function-move_old_files_arch() in usecase_2.py file) 
![Uploading b4 extrac<img width="539" alt="after converting in csv" src="https://user-images.githubusercontent.com/125951139/220279669-2254b802-9da4-4c7f-bf7e-ffa055dc5472.PNG">
<img width="665" alt="after extract" src="https://user-images.githubusercontent.com/125951139/220279673-1e95b7ec-8656-43e3-84d3-4a5ede811fd5.PNG">
t.PNG…](<img width="752" alt="3 crawlers" src="https://user-images.githubusercontent.com/125951139/220279665-b3847064-1434-4424-a895-9df0e53e9230.PNG">
)
