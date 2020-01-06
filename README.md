# AWS-s3-copy-files-between-folders
this is a work in progress a the moment
This lambda script will do the following operation
1)Look for folder key TranLogs/Active in buckets specified.
2)Once found it will run through the files present and split the datepart from it.Example file name :"TestDB_201912050800.trn"
3)Depending on the datepart it will runa comparision to see if the file is older than 7 days.
Note: The code should be able to hadle any files older than 7 days as well
4)depending on the split date on the file, create a folder with the weekend date that the file falls.Example TranLogs/week-of<weekend>
5)Copy the file in that folder and delete from the TranLogs/Active/ folder.
