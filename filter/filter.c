#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define THRESHOLD 50
#define USERID_LENGTH 16
#define LINE_LENGTH 24
#define TUPLE_LENGTH 20
#define BUFFER_CAP 500
#define BUFFER_SIZE TUPLE_LENGTH * BUFFER_CAP
#define BUFFER_BOUND BUFFER_SIZE + TUPLE_LENGTH

char previous[USERID_LENGTH];
char current[USERID_LENGTH];
char line[LINE_LENGTH];
char tuple[TUPLE_LENGTH];
char buffer[BUFFER_SIZE];
char* bpointer = buffer;

void getUser()
{
  int i = 0;
  int k = 0;
  while(line[i] != '\t')
  {
    current[i] = line[i];
    i++;
  }
  current[i] = '\0';
  i++;

  while(line[i] != '\n')
  {
    if (line[i] == '\t')
    {
      tuple[k++] = ',';
    }
    else
    {
      tuple[k++] = line[i];
    }
    i++;
  }
  tuple[k++] = '\t';
  tuple[k] = '\0';
}

int main (int argc,char *argv[])
{

  if (argc != 3)
  {
    printf("Pass in input file and output file!\n");
    return 0;
  }

  FILE *input_file = fopen(argv[1], "r");
  FILE *output_file = fopen(argv[2], "w");
  

  unsigned int count = 0;
  if (input_file != NULL && output_file != NULL)
  {
    if (fgets(line, sizeof(line), input_file) != NULL)
    {
      getUser();
      strcpy(previous,current);
      count = 1;
      strcpy(buffer, current);
      strcat(buffer, "\t");
      strcat(buffer, tuple);
      bpointer = bpointer + strlen(buffer);
    }
    while (fgets(line, sizeof(line), input_file) != NULL )
    {
      getUser();
      if (strcmp(previous,current) != 0)
      {
        if (count > THRESHOLD) 
        {
         if (bpointer > buffer)
         { 
           fwrite(buffer, sizeof(char), bpointer - buffer, output_file);
         }
         fputc('\n', output_file);
        }
        strcpy(previous,current);
        count = 1;
        strcpy(buffer, current);
        strcat(buffer, "\t");
        bpointer = buffer  + strlen(buffer);
      }
      else
      {
        count++;
      }
      strcat(bpointer, tuple);
      bpointer = bpointer + strlen(tuple);
      if (count > THRESHOLD && bpointer - buffer > BUFFER_BOUND) 
      {
        fwrite(buffer, sizeof(char), bpointer - buffer, output_file);
        buffer[0] = '\0';
        bpointer = buffer;
      }
    }
    if (count > THRESHOLD)
    {
      fprintf(output_file, "%s\n", buffer);
    }
    fclose(input_file);
    fclose(output_file);
  }
  else
  {
    printf("File cannot open!\n");
  }
  return 0;
}
