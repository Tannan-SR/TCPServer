#include <iostream>
#include <sstream>
#include <cstring>
#include <map>
#include <queue>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>

#define N 105
using namespace std;
int counter = 0;
queue <int> q;
map <string, string> kv_datastore;
pthread_t client_thread[N];

int lock = 0;
void acquire()
{	
	lock++;
}
void release()
{
	lock--;
}

void* handleClient(void* args);
void* monitor(void*);

int main(int argc, char **argv)
{    

	if (argc != 2)
    {

        fprintf(stderr, "usage: %s <port>\n", argv[0]);

        exit(1);
    }

    int portno = atoi(argv[1]);

    int server_socket, client_socket;

    sockaddr_in server_address, client_address;

    socklen_t client_length;

    server_socket = socket(AF_INET, SOCK_STREAM, 0);

    server_address.sin_family = AF_INET;

    server_address.sin_port = htons(portno);

    server_address.sin_addr.s_addr = INADDR_ANY;

    bind(server_socket, (struct sockaddr *)&server_address, sizeof(server_address));

    listen(server_socket, 5);
    
    pthread_t leader;
    pthread_create(&leader,NULL,&monitor,NULL);
    

    cout << "Listening on port " << portno << endl;
    while (1)
    {

        client_length = sizeof(client_address);

        client_socket = accept(server_socket, (struct sockaddr *)&client_address, &client_length);
        if (client_socket == -1) {
    perror("Error accepting connection");
    	}
        
        cout<<"Client accepted\n";
        
        if(counter<N)
        {
         while(lock);
         acquire();
         counter++;
         release();
        pthread_create(&client_thread[counter], NULL, &handleClient, (void*) &client_socket);

	cout<<"Counter "<<counter<<endl;
	}
	else
	{
		cout<<"Exceeded\n";
		q.push(client_socket);
		cout<<q.front()<<endl;
	}

        
    }
    close(client_socket);
    close(server_socket);

    return 0;
}

void* handleClient(void* args)
{

    char buffer[1024] = {0};
    int client_socket = *((int*)(args)); 


    while (read(client_socket, buffer, sizeof(buffer)) > 0)
    {

        istringstream iss(buffer);

        char line[10] = {0};

        while (iss.getline(line, sizeof(line)))
        {

            if (!strcmp(line, "READ"))
            {

                string response;

                iss.getline(line, sizeof(line));

                auto it = kv_datastore.find(line);

                if (it != kv_datastore.end())
                {

                    response =it->second + "\n";

                    send(client_socket, response.c_str(), response.size(), 0);
                }
                else
                {

                    send(client_socket, "NULL\n", 5, 0);
                }
            }
            else if (!strcmp(line, "WRITE"))
            {

                iss.getline(line, sizeof(line));

                string key = line;

                iss.getline(line, sizeof(line));

                string value = line;

                value.erase(0, 1);
		
		while(lock);
		acquire();
                kv_datastore[key] = value;
                release();
                send(client_socket, "FIN\n", 4, 0);
            }
            else if (!strcmp(line, "COUNT"))
            {

                string response;

                response = kv_datastore.size() + "\n";

                send(client_socket, response.c_str(), response.size(), 0);
            }
            else if (!strcmp(line, "DELETE"))
            {

                iss.getline(line, sizeof(line));
		while(lock);
		acquire();
                if (kv_datastore.erase(line))
                {

                    //cout << "FIN" << endl;

                    send(client_socket, "FIN\n", 4, 0);
                }
                else
                {

                    send(client_socket, "NULL\n", 5, 0);
                }
                release();
            }
            else if (!strcmp(line, "END"))
            {	
            	while(lock);
            	acquire();
            	counter--;
            	release();

		send(client_socket,"\n",1,0);
                close(client_socket);

                shutdown(client_socket, SHUT_RDWR);

                break;
            }
        }
    }
}


void* monitor(void*)
{

	while(1)
	{
		if(!q.empty())
		{
		for (int i = 0; i < N; i++){
				int threadTerminationStatus = pthread_tryjoin_np(client_thread[i], NULL);
				if (threadTerminationStatus == 0){
					pthread_join(client_thread[i], NULL);
					int x = q.front();
					q.pop();
					pthread_create(&client_thread[i], NULL, &handleClient, &x);
					}
				}
		}
	}
}

