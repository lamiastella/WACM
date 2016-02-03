#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "index.h"
#include <fcntl.h>
#include <errno.h>
#include <math.h>

typedef struct{
  pthread_mutex_t *buffer_lock;
  pthread_cond_t *not_full;
  pthread_cond_t *not_empty;
  int maxSize;
  int head;
  int tail;
  int currSize;
  char **buffer;
} bounded_buffer;


typedef struct{
  pthread_cond_t *canRead;
  pthread_cond_t *canWrite;
  pthread_mutex_t *lock;
  int waitWriters;
  int waitReaders;
  int numReaders;
  int numWriters;
} readers_writers;


readers_writers *ht_monitor;
bounded_buffer *file_buffer;

pthread_mutex_t *file_index_lock;
pthread_cond_t *file_indexed;

int doneScanning;
int doneIndexing;
int totalFiles;

void indexer(char *file_name);

int MAX_PATH = 511;
int Num_Files=300;//
int Index=0;//Points the next available location in the array
char **File_Index;//Table containing the list of file names after indexing

void indexer_3(char *file_name) {
  char **New_File_Index;
  int i;
  if(Index==Num_Files-1) {
    Num_Files=Num_Files * 2;
    New_File_Index = realloc(File_Index,Num_Files);
    if(!New_File_Index)
      free(New_File_Index);
    for(i=Index;i<Num_Files;i++) {
      File_Index[i+1]=malloc((MAX_PATH+1)*sizeof (char));
    }
  }
  //update the stats in the array
  strcpy(File_Index[Index],file_name);
  Index++;
}

//Used to add filenames for indexer threads to file_buffer
void *scanner_thread(void *file_list){
  
  //  printf("in scanner thread, no file\n");
  
  FILE *fd = fopen((char *)file_list,"rt");
  char *filename = malloc(sizeof(char) * 513);

  //printf("in scanner thread\n");

  totalFiles = 0;
  
    while(fgets(filename,513,fd) != NULL){
      strtok(filename, "\n\r");

      pthread_mutex_lock(file_buffer->buffer_lock);

      //wait for buffer to not be full
      while(file_buffer->currSize == file_buffer->maxSize)
        pthread_cond_wait(file_buffer->not_full,file_buffer->buffer_lock);

      //enter filename in buffer
      file_buffer->buffer[file_buffer->tail] = filename;
      pthread_cond_broadcast(file_buffer->not_empty);

      //printf("%s",filename);

      //update tail pointer
      file_buffer->tail = (file_buffer->tail + 1) % file_buffer->maxSize;
      file_buffer->currSize = file_buffer->currSize + 1;

      filename = malloc(sizeof(char) * 513);
      
      totalFiles++;

      pthread_mutex_unlock(file_buffer->buffer_lock);
    }

    //    printf("leaving scanner thread\n");

  free(filename);

  
  pthread_mutex_lock(file_buffer->buffer_lock);
  doneScanning = 1;
  pthread_cond_broadcast(file_buffer->not_empty);
  pthread_mutex_unlock(file_buffer->buffer_lock);

  return NULL;
}

void *indexer_thread(){
  
  //printf("in indexer thread\n");

  char *filename;

  while(1){
    //part 1
    pthread_mutex_lock(file_buffer->buffer_lock);

    while(file_buffer->currSize == 0 && doneScanning == 0)
      pthread_cond_wait(file_buffer->not_empty,file_buffer->buffer_lock);

    if(doneScanning == 1 && file_buffer->currSize == 0){
      pthread_mutex_unlock(file_buffer->buffer_lock);
      //printf("Exiting indexer thread.\n");
      return NULL;
    }
    
    filename = file_buffer->buffer[file_buffer->head];
    file_buffer->buffer[file_buffer->head] = NULL;;

    file_buffer->head = (file_buffer->head + 1) % file_buffer->maxSize;
    file_buffer->currSize = file_buffer->currSize - 1;

    pthread_cond_broadcast(file_buffer->not_full);  
    
    pthread_mutex_unlock(file_buffer->buffer_lock);
    //end part 1

    //part 2
    /*
    pthread_mutex_lock(ht_monitor->lock);
    while(ht_monitor->numWriters == 1 || ht_monitor->numReaders > 0){
      ht_monitor->waitWriters++;
      pthread_cond_wait(ht_monitor->canWrite,ht_monitor->lock);
      ht_monitor->waitWriters--;
    }
    ht_monitor->numWriters = 1;
    pthread_mutex_unlock(ht_monitor->lock);
    */

    indexer(filename);

    //printf("index done, pthreadID = %lu\n",(unsigned long)pthread_self());

    /*
    pthread_mutex_lock(ht_monitor->lock);
    ht_monitor->numWriters = 0;
    if(ht_monitor->numReaders > 0)
      pthread_cond_signal(ht_monitor->canRead);
    else
      pthread_cond_signal(ht_monitor->canWrite);
    pthread_mutex_unlock(ht_monitor->lock);
    */
    //end part 2
    
    //part 3
    pthread_mutex_lock(file_index_lock);
    indexer_3(filename);
    pthread_cond_broadcast(file_indexed);

    pthread_mutex_lock(file_buffer->buffer_lock);
    if(doneScanning == 1 && Index == totalFiles)
      doneIndexing = 1;
    pthread_mutex_unlock(file_buffer->buffer_lock);

    pthread_mutex_unlock(file_index_lock);
    //end part 3
  }

  return NULL;
}

void *query_thread(void *input){
  
  /*
  pthread_mutex_lock(ht_monitor->lock);
  int didWait= 0;
  while(ht_monitor->numWriters == 1 || (ht_monitor->waitWriters > 0 
	&& ht_monitor->numReaders > 0 && !didWait)){
    ht_monitor->waitReaders++;
    pthread_cond_wait(ht_monitor->canRead,ht_monitor->lock);
    ht_monitor->waitReaders--;
    didWait = 1;
  }
  ht_monitor->numReaders++;
  pthread_cond_signal(ht_monitor->canRead);
  pthread_mutex_unlock(ht_monitor->lock);
  */

  //query code starts here
  char *user_input = (char*)input;
  
  //parse user input, detect basic/advanced searches
  char *user_input_parsed[5000];
  char *tmp;
  int num_cmds = 0;

  //printf("Input String: %s", user_input);
  strtok(user_input, "\n\r");
  
  tmp = strtok(user_input," ");
    
  while (tmp) 
    {
      user_input_parsed[num_cmds] = tmp;
      num_cmds++;
      tmp = strtok(NULL," ");
    }


  if(num_cmds == 1)
    ;
    //printf("Basic search mode -- search-string: %s\n", user_input_parsed[0]);
  else if(num_cmds == 2){
    //printf("Advanced search mode -- file: %s and search-string: %s\n", user_input_parsed[0], user_input_parsed[1]);
    pthread_mutex_lock(file_index_lock);
    int found = 0;
    int index = 0;
    while(!found){
      if(index == Index)
	pthread_cond_wait(file_indexed,file_index_lock);
      if(strcmp(user_input_parsed[0],File_Index[index]) == 0)
	found = 1;
      index++;
      if(doneIndexing && !found && index == Index){
	printf("ERROR: File %s not found\n",user_input_parsed[0]);
	pthread_mutex_unlock(file_index_lock);

	/*
	pthread_mutex_lock(ht_monitor->lock);
	ht_monitor->numReaders--;
	if(ht_monitor->numReaders == 0)
	  pthread_cond_signal(ht_monitor->canWrite);
	pthread_mutex_unlock(ht_monitor->lock);
	*/
	return NULL;
      }
    }
    pthread_mutex_unlock(file_index_lock);

    char *temp = user_input_parsed[0];
    user_input_parsed[0] = user_input_parsed[1];
    user_input_parsed[1] = temp;
  }
    //end query parsing

  //TEST QUERY CODE

  //////////pthread_mutex_lock(ht_monitor->lock);

  index_search_results_t *result = find_in_index(user_input_parsed[0]);
  
  /////////pthread_mutex_unlock(ht_monitor->lock);

  int advancedFound = 0;

  if(result == NULL)
    printf("Word not found.\n");
  else{
    int i;
    for(i = 0; i < result->num_results; i++){
      if(num_cmds == 1 || (num_cmds == 2 && strcmp(result->results[i].file_name,user_input_parsed[1]) == 0)){
	printf("FOUND: %s %d\n",result->results[i].file_name,result->results[i].line_number);
	if(num_cmds == 2)
	  advancedFound = 1;
      }
    } 
    if(num_cmds == 2 && !advancedFound)
      printf("Word not found.\n");
  }

  //END TEST QUERY CODE

  //query code ends here
  
  /*
  pthread_mutex_lock(ht_monitor->lock);
  ht_monitor->numReaders--;
  if(ht_monitor->numReaders == 0)
    pthread_cond_signal(ht_monitor->canWrite);
  pthread_mutex_unlock(ht_monitor->lock);
  */  

  return NULL;
}

void usage() 
{
  fprintf(stderr, "Usage: search-engine num-indexer-threads file-list\n");
  exit(1);
}

void indexer(char *file_name)
{
  char buffer[1000];
  
  //printf("trying to open file %s\n",file_name);

  strtok(file_name, "\n\r");

  FILE *file = fopen(file_name, "rt");
  if (file == NULL) 
    {
      char error_message[30] = "Can't open file.\n";
      write(STDERR_FILENO, error_message, strlen(error_message));
      return;
      exit(1);
    }
  else
    {
      //printf("File opened for indexing -- %s\n", file_name);
    }

  int count = 0;
  int line_number = 0;

  //the below part leads to a seg fault -- please look into this
  //while (!feof(file))/////////////
  while(fgets(buffer,130,file) != NULL)
  {
      char *word;
      char *saveptr;
      
      //fgets(buffer, 130, file);//////////////
      strtok(buffer, "\n\r");
      
      word = strtok_r(buffer, " \n\t-_!@#$%^&*()_+=,./<>?", &saveptr);

      while (word != NULL) {
	if(strlen(word) >= 3){

	  /*

	  //Writer lock code
	  pthread_mutex_lock(ht_monitor->lock);
	  while(ht_monitor->numWriters == 1 || ht_monitor->numReaders > 0){
	    ht_monitor->waitWriters++;
	    pthread_cond_wait(ht_monitor->canWrite,ht_monitor->lock);
	    ht_monitor->waitWriters--;
	  }
	  ht_monitor->numWriters = 1;
	  pthread_mutex_unlock(ht_monitor->lock);
	  */

	  /////////pthread_mutex_lock(ht_monitor->lock);

	  insert_into_index(word, file_name, line_number);
	  
	  ///////////pthread_mutex_unlock(ht_monitor->lock);

	  /*
	  pthread_mutex_lock(ht_monitor->lock);
	  ht_monitor->numWriters = 0;
	  if(ht_monitor->numReaders > 0)
	    pthread_cond_signal(ht_monitor->canRead);
	  else
	    pthread_cond_signal(ht_monitor->canWrite);
	  pthread_mutex_unlock(ht_monitor->lock);
	  */

	}
	word = strtok_r(NULL, " \n\t-_!@#$%^&*()_+=,./<>?", &saveptr);
      }
      count++;

      line_number = line_number+1;
    }
  fclose(file);

  return;
}
  /*
void fill_up_file_names(char *file_list)
{
  char buffer[513];

  FILE *file;
  file = fopen(file_list, "r");
  if (file == NULL) 
    {
      char error_message[30] = "Can't open file.\n";
      write(STDERR_FILENO, error_message, strlen(error_message));
      exit(1);
    }

  while (!feof(file))
    {
      fgets(buffer, 513, file); //511 chars for file-name + 2 chars to hold a newline and null terminator
      strtok(buffer, "\n\r");
      strcpy(file_names[num_files], buffer); //copy file-names from buffer to the data structure
      num_files++;
    }

  int j; 
  for (j = 0; j < num_files ; j++)
    {
      printf("Extracted filename #%d: %s\n", j+1, file_names[j]);
    }

  fclose(file);
}
  */

int main(int argc, char *argv[])
{
  if(argc != 3) 
    {
      usage();
    }

  int num_threads = atoi(argv[1]);
  char *file_list = argv[2];

  init_index();

  doneScanning = 0;
  doneIndexing = 0;

  //Initialize indexer 3
  int i;
  //char a[10]="file1";
  //char b[10]="file2";
  File_Index = malloc(Num_Files*sizeof(char *));
  for(i=0;i<Num_Files;i++) {
    File_Index[i]=malloc((MAX_PATH+1)*sizeof (char));
  }

  //attr = NULL;

  pthread_rwlockattr_init(&attr);

  //Initialize ht_monitor
  ht_monitor = malloc(sizeof(readers_writers));

  ht_monitor->waitWriters = 0;
  ht_monitor->waitReaders = 0;
  ht_monitor->numReaders = 0;
  ht_monitor->numWriters = 0;
  ht_monitor->canRead = malloc(sizeof(pthread_cond_t));
  ht_monitor->canWrite = malloc(sizeof(pthread_cond_t));
  ht_monitor->lock = malloc(sizeof(pthread_mutex_t));
  pthread_mutex_init(ht_monitor->lock,NULL);

  //init file index lock
  file_index_lock = malloc(sizeof(pthread_mutex_t));
  file_indexed = malloc(sizeof(pthread_cond_t));

  ///CODE ADDED BY ETHAN///
  //Initialize filebuffer
  file_buffer = malloc(sizeof(bounded_buffer));

  file_buffer->buffer_lock = malloc(sizeof(pthread_mutex_t));
  pthread_mutex_init(file_buffer->buffer_lock,NULL);

  file_buffer->not_full = malloc(sizeof(pthread_cond_t));
  file_buffer->not_empty = malloc(sizeof(pthread_cond_t));

  file_buffer->maxSize = 10;
  file_buffer->head = 0;
  file_buffer->tail = 0;
  file_buffer->currSize = 0;

  file_buffer->buffer = malloc(sizeof(char *) * file_buffer->maxSize);

  pthread_t scanner;
  int pid_scanner;
  pthread_t *indexer = malloc(sizeof(pthread_t) * num_threads);
  int *pid_indexer = malloc(sizeof(int) * num_threads);

  pid_scanner = pthread_create(&scanner,NULL,scanner_thread,(void *)file_list);

  for(i = 0; i < num_threads; i++)
    pid_indexer[i] = pthread_create(&(indexer[i]),NULL,indexer_thread,NULL);

  char *user_input = malloc(sizeof(char) * 130);
  

  int numQuery = 0;
  pthread_t *query = malloc(sizeof(pthread_t) * 10000);
  int pid_query;

  while(fgets(user_input,128,stdin) != NULL){    
    pid_query = pthread_create(&query[numQuery],NULL,query_thread,(void *)user_input);
    user_input = malloc(sizeof(char) * 130);
    numQuery++;
  }

    

	///END CODE ADDED BY ETHAN///

	/*
	

  //a data structure to pass file names from the file system scanner to the indexer  
  file_names = (char**) malloc(1000000*sizeof(char*));  //supports 100,000 file names
  int i;
  for (i = 0; i < 1000000; i++)
    {
      file_names[i] = (char*) malloc(511*sizeof(char));  //file names have been specified to be less than 511 characters (defined as MAXPATH in the provided code)
    } 

  fill_up_file_names(file_list);  

  int j;
  for (j = 0; j < num_files ; j++)
      indexer(file_names[j]);

  //declaring an array for 'num_threads' number of threads.
  pthread_t* thread_id = (pthread_t*) calloc(num_threads, sizeof(pthread_t));

  //creating 'num' number of threads with default attributes and the common function -- 'test_func' for execution
  int i, j;
  for(i = 0; i < num_threads; i++)
    {
      pthread_create(&thread_id, NULL, test_func, NULL);
    }

  for(j = 0; j < num_threads; j++)
    {
      pthread_join(thread_id[j], NULL);
    }

  free(thread_id);
	
	*/

  int j;
  
  for(j = 0; j < num_threads; j++)
    {
      pthread_join(indexer[j], NULL);
    }

  free(indexer);

  for(j = 0; j < numQuery; j++)
    {
      pthread_join(query[j], NULL);
    }

  /*
  exitThreads = 1;
  pthread_mutex_lock(file_buffer->buffer_lock);
  pthread_cond_broadcast(file_buffer->not_empty);
  pthread_cond_broadcast(file_buffer->not_full);
  pthread_mutex_unlock(file_buffer->buffer_lock);

  */

  free(file_buffer->not_full);
  free(file_buffer->not_empty);
  free(file_buffer->buffer_lock);
  free(file_buffer);

  free(ht_monitor->canRead);
  free(ht_monitor->canWrite);
  free(ht_monitor->lock);
  free(ht_monitor);

  free(File_Index);
  
  return 0;

}
