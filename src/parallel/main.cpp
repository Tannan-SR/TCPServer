#include <stdio.h>

#include <stdlib.h>

#include <pthread.h>

#include <string.h>

#include <string>

#include <netinet/in.h>

#include <unistd.h>

#include <sys/socket.h>

#include <unordered_map>

#include <sstream>

#include <iostream>

#include <vector>

#include <sys/socket.h>

#include <arpa/inet.h>

#include <queue>

#include <chrono>

#include <thread>

using namespace std;

struct Thread

{

  pthread_t thread_id;

  int socketID;
};

struct ThreadArgs

{

  struct Thread *array;

  size_t size;
};

queue<int> q;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t active = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t pool_lock = PTHREAD_MUTEX_INITIALIZER;

int active_threads = 0;

unordered_map<string, string> kv_datastore;

void write_into(int *pointer, string &key, string &value)

{

  value.erase(value.begin(), value.begin() + 1);

  kv_datastore[key] = value;

  *pointer++;

  *pointer++;
}

void read_from(string &key, char *output)

{

  if (kv_datastore.find(key) == kv_datastore.end())

  {

    strcat(output, "NULL\n");
  }

  else

  {

    strcat(output, kv_datastore.find(key)->second.c_str());

    strcat(output, "\n");
  }
}

void gimme_count(char *output)

{

  int count = kv_datastore.size();

  string str_count = to_string(count);

  strcat(output, str_count.c_str());

  strcat(output, "\n");
}

void remove(string &key, char *output)

{

  int ret = kv_datastore.erase(key);

  if (ret == 0)

  {

    string res = "NULL\n";

    strcat(output, res.c_str());
  }

  else

  {

    string res = "FIN\n";

    strcat(output, res.c_str());
  }
}

int handleClient(char *input, char *output)

{

  output[0] = '\0';

  char delim = '\n';

  istringstream ss(input);

  string token;

  vector<string> tokens;

  while (getline(ss, token, delim))

  {

    tokens.push_back(token);
  }

  for (int i = 0; i < tokens.size(); i++)

  {

    auto &str = tokens[i];

    if (str == "WRITE")

    {

      auto &key = tokens[i + 1];

      auto &val = tokens[i + 2];

      pthread_mutex_lock(&lock);

      write_into(&i, key, val);

      pthread_mutex_unlock(&lock);

      strcat(output, "FIN\n");
    }

    else if (str == "READ")

    {

      auto &key = tokens[i + 1];

      pthread_mutex_lock(&lock);

      read_from(key, output);

      pthread_mutex_unlock(&lock);

      i++;
    }

    else if (str == "COUNT")

    {

      pthread_mutex_lock(&lock);

      gimme_count(output);

      pthread_mutex_unlock(&lock);
    }

    else if (str == "DELETE")

    {

      auto &key = tokens[i + 1];

      pthread_mutex_lock(&lock);

      remove(key, output);

      pthread_mutex_unlock(&lock);

      i++;
    }

    else if (str == "END")

    {

      strcat(output, "\n");

      return -1;
    }
  }

  return 0;
}

void *serve(void *randomm)

{

  int socketID = *((int *)randomm);

  while (1)

  {

    char buffer[1024];

    int val = read(socketID, buffer, 1024);

    if (val > 0)

    {

      char response[1024];

      int returnval = handleClient(buffer, response);

      if (returnval == -1)

      {

        char buff[100] = {"recieved message\n"};

        strcpy(buff, response);

        write(socketID, buff, strlen(buff));

        cout << "Terminating" << pthread_self() << endl;

        close(socketID);

        shutdown(socketID, SHUT_RDWR);

        pthread_mutex_lock(&active);

        active_threads--;

        pthread_mutex_unlock(&active);

        break;
      }
    }
  }

  pthread_exit(NULL);
}

void *handleClient(void *args)

{

  struct ThreadArgs *threadArgs = static_cast<ThreadArgs *>(args);

  struct Thread *thread_pool = threadArgs->array;

  while (1)

  {

    pthread_mutex_lock(&mutex);

    int val = !q.empty();

    pthread_mutex_unlock(&mutex);

    if (val)

    {

      for (int i = 0; i < 8; i++)

      {

        pthread_mutex_lock(&pool_lock);

        int threadTerminationStatus = pthread_tryjoin_np(thread_pool[i].thread_id, NULL);

        pthread_mutex_unlock(&pool_lock);

        if (threadTerminationStatus == 0)

        {

          pthread_join(thread_pool[i].thread_id, NULL);

          pthread_mutex_lock(&mutex);

          int fontele = q.front();

          q.pop();

          pthread_mutex_unlock(&mutex);

          pthread_mutex_lock(&pool_lock);

          thread_pool[i].socketID = fontele;

          pthread_create(&thread_pool[i].thread_id, NULL, &serve, &thread_pool[i].socketID);

          pthread_mutex_unlock(&pool_lock);
        }
      }
    }

    usleep(1000);
  }
}

int main(int argc, char **argv)

{

  int portno;

  if (argc != 2)

  {

    fprintf(stderr, "usage: %s <port>\n", argv[0]);

    exit(1);
  }

  portno = atoi(argv[1]);

  struct sockaddr_in server, client;

  int client_len, new_socket;

  client_len = sizeof(server);

  char buffer[1500];

  int sock = socket(AF_INET, SOCK_STREAM, 0);

  memset((char *)&server, 0, sizeof(server));

  server.sin_family = AF_INET;

  server.sin_addr.s_addr = htonl(INADDR_ANY);

  server.sin_port = htons(portno);

  bind(sock, (struct sockaddr *)&server, sizeof(server));

  cout << "Listening at port no" << portno << endl;

  listen(sock, 100);

  pthread_t t;

  int thread_number = 10;

  struct Thread thread_pool[thread_number];

  struct ThreadArgs threadArgs;

  threadArgs.array = thread_pool;

  threadArgs.size = thread_number;

  pthread_create(&t, NULL, &handleClient, &threadArgs);

  int returnvalue = 0;

  while (1)

  {

    new_socket = accept(sock, (struct sockaddr *)&client, (socklen_t *)&client_len);

    pthread_mutex_lock(&active);

    int actno = active_threads++;

    pthread_mutex_unlock(&active);

    if (new_socket >= 0 && actno < thread_number)

    {

      pthread_mutex_lock(&pool_lock);

      thread_pool[actno].socketID = new_socket;

      pthread_create(&thread_pool[actno].thread_id, NULL, &serve, &thread_pool[actno].socketID);

      pthread_mutex_unlock(&pool_lock);
    }

    else

    {

      pthread_mutex_lock(&mutex);

      q.push(new_socket);

      pthread_mutex_unlock(&mutex);
    }
  }

  close(sock);

  shutdown(sock, SHUT_RDWR);

  return 0;
}
