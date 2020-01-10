By default you should not change those functions of the PagedFileManager,
FileHandle, and RecordBasedFileManager classes defined in rbf/pfm.h and rbf/rbfm.h.
If you think some changes are really necessary, please contact us first.

If you are not using CLion and want to use command line make tool:

 - Modify the "CODEROOT" variable in makefile.inc to point to the root
  of your code base if you can't compile the code.
 
 - Implement the Record-based Files (RBF) Component:

   Go to folder "rbf" and type in:

   ```
   make clean
   make
   ./rbftest_01         
   ```


   The program should run. But it will generates an error. You are supposed to
   implement the API of the paged file manager defined in pfm.h and some
   of the methods in rbfm.h as explained in the project description.