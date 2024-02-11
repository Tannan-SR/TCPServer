#include <iostream>

#include <sstream>

#include <cstring>

#include <map>

#include <unistd.h>

#include <sys/socket.h>

#include <netinet/in.h>

#include <pthread.h>

using namespace std;

// Function to handle client requests

void handleClient(int client_socket, map<string, string> &kv_datastore)
{

    char buffer[1024] = {0};

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

                kv_datastore[key] = value;
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

                if (kv_datastore.erase(line))
                {

                    //cout << "FIN" << endl;

                    send(client_socket, "FIN\n", 4, 0);
                }
                else
                {

                    send(client_socket, "NULL\n", 5, 0);
                }
            }
            else if (!strcmp(line, "END"))
            {

		send(client_socket,"\n",1,0);
                close(client_socket);

                shutdown(client_socket, SHUT_RDWR);

                break;
            }
        }
    }
}

void *handleClientWrapper(void *arg) {
    int client_socket = *((int *)arg);
    map<string, string> kv_datastore;
    handleClient(client_socket, kv_datastore);
    close(client_socket);
    pthread_exit(NULL);
}


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

    listen(server_socket, 1);

    cout << "Listening on port " << portno << endl;
    map<string, string> kv_datastore;
    while (1)
    {

        client_length = sizeof(client_address);

        client_socket = accept(server_socket, (struct sockaddr *)&client_address, &client_length);
        
        

        if (client_socket < 0)
        {

            cerr << "Error accepting request from client!" << endl;

            exit(1);
        }

                handleClient(client_socket, kv_datastore);

        close(client_socket);
    }

    close(server_socket);

    return 0;
}

