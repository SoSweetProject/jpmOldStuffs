/* Hello World program */

#include<stdio.h>

struct node
{
  char n;
  struct node *sons[10];
};


void insert(node *tree, char *id)
{
  char digit = id[0] - '0'
  if id[1]=='\0'
  {
    if sons[digit]==NULL
    {
      sons[digit]=malloc(sizeof(node));
      sons[digit]->n=-1
    }
    sons[digit]->n+=1
    return sons[digit]->n
  }
  else
  {
    if sons[digit]==NULL
    {
      sons[digit]=malloc(sizeof(node));
      sons[digit]->n=0
      for (i=0;i<10;i++){
        sons[digit]->sons[i]=NULL
      }
    return insert(sons[digit],id[1])
    }
  }
};

int main(void)
{

};
