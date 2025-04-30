#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <errno.h>
#include <ctype.h>
#include <time.h>
#include <pthread.h>
#include <arpa/inet.h>

// constants for game
#define MAX_PLAYERS 5
#define NAMESIZE 255
#define BUFSIZE 256
#define LISTENING_DEPTH 5
#define ROUNDS 5
#define MIN_PLAYERS_TO_CONTINUE 1 // Game will end if there is only one player

// data structure for player 
typedef struct {
    int fd;                     // socket file descriptor
    struct sockaddr_in addr;    // address info of client
    char name[NAMESIZE];        // name of player
    char move;                  // move (R,P,S,L,K)
    int score;                  // score of player
    volatile int active;        // player connected in the game? (volatile for thread safety)
    volatile int ready;         // did player submit move for the round?
    pthread_t thread;           // handler thread ID for player
} Player;

// global game variables
Player players[MAX_PLAYERS];                                 // to store player data
volatile int curr_round = 0;                                 // ongoing game round
volatile int active_players = 0;                             // no. of connected players 
pthread_mutex_t lock;                                        // mutex to protect shared player data
pthread_cond_t round_cond = PTHREAD_COND_INITIALIZER;        // variable for game loop to wait for player moves
volatile int game_started = 0;                               // flag to show if the game is in progress 
pthread_cond_t client_round_cond = PTHREAD_COND_INITIALIZER; // variable for clients to wait for round end
pthread_mutex_t client_round_lock = PTHREAD_MUTEX_INITIALIZER; // mutex for client round waiting
pthread_cond_t game_start_cond = PTHREAD_COND_INITIALIZER;   // variable to signal game start


// Convrsion to full name of the move
char *get_move_name(char move) {
    switch (move) {
        case 'L': return "Lizard";
        case 'K': return "Spock";
        case 'R': return "Rock";
        case 'S': return "Scissors";
        case 'P': return "Paper";
        default: return "Invalid Move";
    }
}

// Returns: 1 if move1 wins, -1 if move2 wins, 0 for a tie
int determine_winner(char mv1, char mv2) {
    if (mv1 == mv2) return 0; // Tie
    if ((mv1 == 'L' && (mv2 == 'K' || mv2 == 'P')) ||
        (mv1 == 'K' && (mv2 == 'S' || mv2 == 'R')) ||
        (mv1 == 'R' && (mv2 == 'S' || mv2 == 'L')) ||
        (mv1 == 'S' && (mv2 == 'P' || mv2 == 'L')) ||
        (mv1 == 'P' && (mv2 == 'R' || mv2 == 'K')))
        return 1; // move1 wins
    return -1;    // move2 wins
}

// Sends a message to all active players
void broadcast(const char *msg, int exclude_fd) {
    pthread_mutex_lock(&lock); // locking access to players
    for (int i = 0; i < MAX_PLAYERS; i++) {
        if (players[i].active && players[i].fd != -1 && players[i].fd != exclude_fd) {
            if (send(players[i].fd, msg, strlen(msg), 0) < 0) {
                // Error sending; considering logging or marking player inactive if persistent
            }
        }
    }
    pthread_mutex_unlock(&lock);
}

// Checks if all currently active players have submitted their moves for the round
int all_players_ready() {
    int ready_cnt = 0;
    int curr_active_cnt = 0;
    for (int i = 0; i < MAX_PLAYERS; i++) {
        if (players[i].active) {
            curr_active_cnt++;
            if (players[i].ready) {
                ready_cnt++;
            }
        }
    }
    if (curr_active_cnt == 0) return 0; // No players left
    return (ready_cnt == curr_active_cnt);
}

//Threads ftn
// It will handles communication with a single connected client
void *handle_client(void *arg) {
    int idx = *(int *)arg;    // getting player index from arg
    free(arg);                  // free malloc'd index
    int fd = -1;
    char curr_name[NAMESIZE] = "Unknown"; // local name copy

    // getting initial data (fd, name) safely
    pthread_mutex_lock(&lock);
    fd = players[idx].fd;
    strcpy(curr_name, players[idx].name);
    pthread_mutex_unlock(&lock);

    if (fd == -1) {
        fprintf(stderr, "error: invalid fd player idx %d\n", idx);
        pthread_exit(NULL);
    }

    char buffer[BUFSIZE];
    int n;

    // receive player's name
    n = recv(fd, buffer, BUFSIZE - 1, 0);
    if (n <= 0) {
        fprintf(stderr, "Before sending name, player at idx %d got disconnected.\n", idx);
        close(fd);
        pthread_mutex_lock(&lock);
        if (players[idx].active) { active_players--; } // decrementing only if counted
        players[idx].active = 0;
        players[idx].fd = -1;
        pthread_mutex_unlock(&lock);
        pthread_exit(NULL);
    }
    buffer[n] = '\0';
    buffer[strcspn(buffer, "\r\n")] = 0; // removing trailing newline/CR

    // updating player struct with name and announce join (under lock)
    pthread_mutex_lock(&lock);
    strncpy(players[idx].name, buffer, NAMESIZE - 1);
    players[idx].name[NAMESIZE - 1] = '\0'; // ensuring null termination
    strcpy(curr_name, players[idx].name); // updating local copy

    printf("\n%s (Player %d) joined! at Addr: %s Port: %d\n",
           players[idx].name, idx + 1,
           inet_ntoa(players[idx].addr.sin_addr),
           ntohs(players[idx].addr.sin_port));

    // send waiting message if game is not full, or starting message if it is
    char status_msg[BUFSIZE];
    if (active_players < MAX_PLAYERS) {
        snprintf(status_msg, BUFSIZE, "%d players remaining to join! \n(M=Score, Q=Quit, T=Reset Scores)\n", MAX_PLAYERS - active_players);
    } else {
        snprintf(status_msg, BUFSIZE, "All players joined the GAME! Let's start the game...!\n(M=Score, Q=Quit, T=Reset Scores)\n");
    }
    if (send(fd, status_msg, strlen(status_msg), 0) < 0) {
        perror("send status message got failed");
    }

    // wait for the game loop to signal the start of the game
    printf("%s is waiting to start game.!!\n", curr_name);
    while (!game_started) {
        pthread_cond_wait(&game_start_cond, &lock); // waiting for the signal from game_loop
    }
    printf("%s notified about start game!\n", curr_name); 
    pthread_mutex_unlock(&lock); // releasing lock after checking game_started

    // main game interaction loop
    while (1) {
        pthread_mutex_lock(&lock);
        // checking if player disconnected between rounds or game ended
        if (curr_round >= ROUNDS || !players[idx].active || !game_started) {
            pthread_mutex_unlock(&lock);
            break; // exit loop
        }
        int round_num = curr_round; // capturing round number for this particular iteration
        pthread_mutex_unlock(&lock);

        // sending round prompt
        char round_msg[BUFSIZE];
        snprintf(round_msg, BUFSIZE, "\nRound %d/%d\nChoose (R/P/S/L/K/Q/M/T): ", round_num + 1, ROUNDS);
        if (send(fd, round_msg, strlen(round_msg), 0) < 0) {
            fprintf(stderr, "sending promt for round failed for %s\n", curr_name);
            break; // exit loop on send failure
        }

        // receive player's input 
        n = recv(fd, buffer, BUFSIZE - 1, 0);
        if (n <= 0) {
            fprintf(stderr, "%s got disconnected during round %d.\n", curr_name, round_num + 1);
            if (n < 0) perror("recieved failed details");
            break; 
        }
        buffer[n] = '\0';
        char input = toupper(buffer[0]);

        // processing input
        if (input == 'Q') {
            printf("%s is choosing to quit the game.\n", curr_name);
            break; 
        } else if (input == 'M') { // show current game score
            pthread_mutex_lock(&lock);
            char score_msg[BUFSIZE];
            snprintf(score_msg, BUFSIZE, "Your current game score is : %d\n", players[idx].score);
            pthread_mutex_unlock(&lock);
            if (send(fd, score_msg, strlen(score_msg), 0) < 0) {
                fprintf(stderr, "send score detils got failed for %s\n", curr_name);
                break;
            }
            continue; // re-prompting for input for the same round
        } else if (input == 'T') { // reset all scores values for all players
            pthread_mutex_lock(&lock);
            printf("%s has requested to reset score values.\n", curr_name);
            for (int i = 0; i < MAX_PLAYERS; i++) {
                if (players[i].active) { players[i].score = 0; }
            }
            pthread_mutex_unlock(&lock);
            broadcast("score values have been reset by a player.\n", -1); // broadcast calls lock internally
            continue; // re-prompting for input for the same round
        } else if (strchr("RPSLK", input)) { // when player is entering the valid game move
            pthread_mutex_lock(&lock);
            if (!players[idx].active) { // first check, if disconnected while choosing
                pthread_mutex_unlock(&lock);
                break;
            }
            players[idx].move = input;
            players[idx].ready = 1;
            printf("%s has played move %c for %d round.\n", curr_name, input, round_num + 1); 
            pthread_cond_signal(&round_cond); // send signal to game loop that a player is ready
            pthread_mutex_unlock(&lock);

            // wait for this round to end before starting next loop iteration
            pthread_mutex_lock(&client_round_lock);
            printf("%s is waiting for results of round %d ...!\n", curr_name, round_num + 1); 
            pthread_mutex_lock(&lock); // need main lock to check the player state
            while (players[idx].ready && players[idx].active && curr_round < ROUNDS && game_started) {
                pthread_mutex_unlock(&lock); // unlocking main lock before waiting on client CV
                pthread_cond_wait(&client_round_cond, &client_round_lock);
                pthread_mutex_lock(&lock); // re-acquiring main lock after wake-up to re-check conditions
            }
            pthread_mutex_unlock(&lock); // unlocking main lock
            printf("%s moving forward after completing round %d.\n", curr_name, round_num + 1); 
            pthread_mutex_unlock(&client_round_lock);

        } else { // when invalid input
            char invalid_msg[] = "Invalid input. Please choose R/P/S/L/K/Q/M/T.\n";
            if (send(fd, invalid_msg, strlen(invalid_msg), 0) < 0) {
                fprintf(stderr, "send invalid choice failed for %s\n", curr_name);
                break;
            }
            continue; // re-prompting for input for the same round
        }
    } 

    // cleanup client thread
    printf("Thread for %s (idx: %d) has been cleaned up!!\n", curr_name, idx);
    close(fd);
    pthread_mutex_lock(&lock);
    if (players[idx].active) { // only decrement count if player is active 
        active_players--;
        printf("Decremented count for active players now: %d\n", active_players);
    }
    players[idx].active = 0; // marking inactive
    players[idx].fd = -1;    // marking fd as invalid
    players[idx].ready = 0;    // ensuring not ready state

    pthread_cond_signal(&round_cond); // send signal to game loop in case it is waiting specifically for this player
    pthread_cond_broadcast(&client_round_cond); // waking up any other clients who are potentially stuck

    // check if the game should end due to lack of players
    if (game_started && active_players < MIN_PLAYERS_TO_CONTINUE && curr_round < ROUNDS) {
        printf("Server is ending game early due to insufficient players.\n");
        pthread_cond_signal(&round_cond); // send signalto game loop to re-check player count
    }
    pthread_mutex_unlock(&lock);
    return NULL;
}


// main game logic thread - it will manage rounds, scoring, and game state
void *game_loop(void *arg) {
    (void)arg; 
    printf("Game loop thread has been started.\n");

    while (1) { // main game round loop
        pthread_mutex_lock(&lock);

        // checking the Game Termination Conditions
        if (curr_round >= ROUNDS) {
            printf("Game loop: all rounds done.\n");
            pthread_mutex_unlock(&lock); break;
        }
        if (active_players < MIN_PLAYERS_TO_CONTINUE) {
            printf("Game loop: Not enough player count to continue the game (%d active).\n", active_players);
             pthread_mutex_unlock(&lock);
             broadcast("Game ended: Not enough players are available.\n", -1);
             break;
        }
        if (!game_started) {
            printf("Game loop: Game stopped externally.\n");
            pthread_mutex_unlock(&lock); break;
        }

        // Waiting for player's moves
        printf("Game loop: round %d started. Waiting for %d players...\n", curr_round + 1, active_players);
        while (!all_players_ready() && game_started && active_players >= MIN_PLAYERS_TO_CONTINUE) {
            pthread_cond_wait(&round_cond, &lock); // waiting for signals from handle_client
            //printf("Game loop: Woke up, checking player readiness...\n"); 
        }

        // re-checking conditions after waiting
        if (active_players < MIN_PLAYERS_TO_CONTINUE) {
            printf("Game loop: enough players are not there after waiting for moves (%d active).\n", active_players);
             pthread_mutex_unlock(&lock);
             broadcast("Game ended: A player got disconnected during round.\n", -1);
             break;
        }
        if (!game_started) { // for safety check
            printf("Game loop: Game stopped while waiting for moves.\n");
            pthread_mutex_unlock(&lock); break;
        }

        // processing round results 
        printf("Game loop: All %d players are ready for playing the round %d. Processing...\n", active_players, curr_round + 1);
        char res_msg[BUFSIZE * (MAX_PLAYERS + 2)]; // buffer for round results message
        snprintf(res_msg, sizeof(res_msg), "\n=== RESULTS AFTER ROUND %d ===\n", curr_round + 1);

        int round_wins[MAX_PLAYERS] = {0}; // tracking wins per player this round

        // calculating wins for each player against others(considering each pair (just like brute-force))
        for (int i = 0; i < MAX_PLAYERS; i++) {
            if (!players[i].active) continue;
            for (int j = i + 1; j < MAX_PLAYERS; j++) { // comparing i vs j
                if (!players[j].active) continue;
                int outcome = determine_winner(players[i].move, players[j].move);
                if (outcome == 1) round_wins[i]++; // i wins
                else if (outcome == -1) round_wins[j]++; // j wins
            }
        }

        // updating scores and build result message string
        for (int i = 0; i < MAX_PLAYERS; i++) {
            if (!players[i].active) continue;
            if (round_wins[i] > 0) {
                printf("%s (%s) won %d matchups. Adding %d to final score. (Prev score: %d)\n", players[i].name, get_move_name(players[i].move), round_wins[i], round_wins[i], players[i].score); // Debug
                players[i].score += round_wins[i]; // adding points equal to number of wins
            }
             // append player's round info to the result message
            snprintf(res_msg + strlen(res_msg), sizeof(res_msg) - strlen(res_msg),
                     "%s: %s (Current_Score: %d)\n",
                     players[i].name, get_move_name(players[i].move), players[i].score);
        }

        // preparing for the next round
        curr_round++;
        for (int i = 0; i < MAX_PLAYERS; i++) {
            if (players[i].active) {
                players[i].ready = 0;   // reset the ready flag
                players[i].move = '\0'; // clearing prev move
            }
        }
        printf("Game loop: Round %d has been finished.\n", curr_round); // curr_round is now the *next* round number because we incremented before.

        // unlock main lock before broadcasting results
        pthread_mutex_unlock(&lock);

        broadcast(res_msg, -1); // send round scores to all players

        // send signal to client threads that the round processing is completed..
        pthread_mutex_lock(&client_round_lock);
        pthread_cond_broadcast(&client_round_cond);
        pthread_mutex_unlock(&client_round_lock);
    }

    printf("Game loop thread has been finished.\n");

    // game over processing 
    pthread_mutex_lock(&lock);

    if (game_started) { // only processin final results if game played
        char final_result[BUFSIZE * (MAX_PLAYERS + 3)]; // buffer for the final message
        strcpy(final_result, "\n===== GAME IS OVER =====\nFINAL SCORES ARE :\n");
        int players_ever_joined = 0;
        printf("Showing final scores...\n");

        int maxi = -1; // for tracking highest score
        char winner_names[MAX_PLAYERS * NAMESIZE + MAX_PLAYERS] = ""; // store name(s) of winner(s)(in case joint winner)
        int winner_cnt = 0;

        // iterating over all players slots to find final scores and potential winner(s)
        // includes players who quit game middle way but played some rounds
        for (int i = 0; i < MAX_PLAYERS; i++) {
            if (players[i].name[0] != '\0') { // check if slot was ever used 
                players_ever_joined++;
                printf("  Final Score - %s: %d\n", players[i].name, players[i].score);
                snprintf(final_result + strlen(final_result), sizeof(final_result) - strlen(final_result),
                         "%s: %d\n", players[i].name, players[i].score);

                // checkinf for the winner
                if (players[i].score > maxi) { // getting new high score
                    maxi = players[i].score;
                    strcpy(winner_names, players[i].name);
                    winner_cnt = 1;
                } else if (players[i].score == maxi && maxi >= 0) { // tie for high score
                    strcat(winner_names, ", ");
                    strcat(winner_names, players[i].name);
                    winner_cnt++;
                }
            }
        }

        // joining winner announcement to the final message
        if (players_ever_joined > 0 && maxi >= 0) {
            if (winner_cnt > 1) {
                snprintf(final_result + strlen(final_result), sizeof(final_result) - strlen(final_result),
                         "\nWinners (Tie game between): %s with %d points!\n", winner_names, maxi);
            } else {
                snprintf(final_result + strlen(final_result), sizeof(final_result) - strlen(final_result),
                         "\nWinner: %s with %d points!\n", winner_names, maxi);
            }
        } else if (players_ever_joined > 0) {
            strcat(final_result, "\nNo winner (all scores zero).\n");
        }

        // unlocking before broadcast
        pthread_mutex_unlock(&lock);
        if (players_ever_joined > 0) {
            printf("Broadcasting final results everywhere:\n%s", final_result);
            broadcast(final_result, -1);
        } else {
            printf("No players ever joined properly\n");
        }
        pthread_mutex_lock(&lock); // re-acquiring lock to modify game_started

    } else {
        printf("Game did not start correctly.\n");
    }

    game_started = 0; // marking game as officially over

    pthread_mutex_unlock(&lock); // unlock main lock before sending signals to clients

    // waking up any client threads that might still be waiting (e.g., if game ended early)
    pthread_mutex_lock(&client_round_lock);
    pthread_cond_broadcast(&client_round_cond);
    pthread_mutex_unlock(&client_round_lock);

    return NULL;
}


// Server Setup and Main Accept Loop 

// initializing the server socket and managing incoming connections
void server(int port) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket failed"); exit(EXIT_FAILURE); }

    // allowig address reuse immediately after server stops
    int optVal = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &optVal, sizeof(optVal));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = INADDR_ANY, // Listen on all interfaces
        .sin_port = htons(port)
    };
    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind failed"); close(server_fd); exit(EXIT_FAILURE);
    }
    if (listen(server_fd, LISTENING_DEPTH) < 0) {
        perror("listen failed"); close(server_fd); exit(EXIT_FAILURE);
    }

    printf("Server is running on port %d\n", port);

    // initializing mutexes and player array
    pthread_mutex_init(&lock, NULL);
    pthread_mutex_init(&client_round_lock, NULL);
    for (int i = 0; i < MAX_PLAYERS; i++) {
        players[i].fd = -1; players[i].active = 0; players[i].ready = 0;
        players[i].score = 0; players[i].name[0] = '\0';
    }

    pthread_t game_thread;           // ID for the game loop thread
    int game_thread_up = 0;     // flag: has game thread been started?

    // main loop to accept new client connections
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &len);
        if (client_fd < 0) {
            if (errno == EINTR) continue; // ignoring interrupted system call
            perror("accept failed");
            continue; // try to accept next connection
        }

        pthread_mutex_lock(&lock); // locking before accessing shared data

        // check if server is full or game is already running
        if (active_players >= MAX_PLAYERS) {
             fprintf(stderr, "(%d) players joined. Nowonwards rejecting connections.\n", MAX_PLAYERS);
             send(client_fd, "Server is full. Try again later.\n", 30, 0);
             close(client_fd);
             pthread_mutex_unlock(&lock);
             continue;
        }
        if (game_started) {
             fprintf(stderr, "Game is in progress.\n");
             send(client_fd, "Server is busy. Game in progress.\n", 30, 0);
             close(client_fd);
             pthread_mutex_unlock(&lock);
             continue;
        }

        // finding an empty player slot
        int curr_player_idx = -1;
        for (int i = 0; i < MAX_PLAYERS; ++i) {
            if (players[i].fd == -1 && !players[i].active) {
                curr_player_idx = i;
                break;
            }
        }
        if (curr_player_idx == -1) { // it should not happen if checks above are correct
            fprintf(stderr, "No any free player slots available!\n");
            close(client_fd);
            pthread_mutex_unlock(&lock);
            continue;
        }

        // allocating memory for the thread argument (player index)
        int *client_index_ptr = malloc(sizeof(int));
        if (!client_index_ptr) {
            perror("malloc got failed for client index..");
            close(client_fd);
            pthread_mutex_unlock(&lock);
            continue;
        }
        *client_index_ptr = curr_player_idx;

        // initializing player slot
        players[curr_player_idx] = (Player){
            .fd = client_fd, .addr = client_addr, .name = "", .move = '\0',
            .score = 0, .active = 1, .ready = 0, .thread = 0
        };
        players[curr_player_idx].name[0] = '\0'; // ensurin name is empty initially
        active_players++; // incrementing active player count 
        printf("Server has accepted connection from player index at %d. Now Total Active Players: %d\n", curr_player_idx, active_players);

        // create and detach the client handler thread
        if (pthread_create(&players[curr_player_idx].thread, NULL, handle_client, client_index_ptr) != 0) {
            perror("pthread_create got failed for handle_client");
            close(client_fd);
            players[curr_player_idx].active = 0; // revert state
            players[curr_player_idx].fd = -1;
            active_players--;
            free(client_index_ptr); // freeing allocated index memory
        } else {
             pthread_detach(players[curr_player_idx].thread); // detach successful thread
        }

        // check if the game should start now (only if lobby is full)
        if (active_players == MAX_PLAYERS && !game_started && !game_thread_up) {
            printf("Maximum players count reached. Lets start the game loop...\n");
            curr_round = 0;       // reset round counter
            game_started = 1;        // settinng game started flag
            game_thread_up = 1; // preventing start of multiple game threads

            // reset scores and state for all players at the start of the game
            for (int i = 0; i < MAX_PLAYERS; i++) { players[i].score = 0; players[i].ready = 0; }

            // create and detach the main game loop thread
            if (pthread_create(&game_thread, NULL, game_loop, NULL) != 0) {
                perror("!! Error: pthread_create game_loop got failed");
                game_started = 0; // Game failed to start
                game_thread_up = 0;
                broadcast("Error in starting the game. Disconnecting.\n", -1); // using its own lock
                // cleanup connections on critical failure
                for (int i = 0; i < MAX_PLAYERS; ++i) {
                     if (players[i].active) close(players[i].fd);
                     players[i].active = 0; players[i].fd = -1; players[i].name[0] = '\0';
                 }
                active_players = 0;
            } else {
                 pthread_detach(game_thread); // detach successful game thread
                 printf("Broadcasting game start signal...\n");
                 pthread_cond_broadcast(&game_start_cond); // signal waiting clients
            }
        }
        pthread_mutex_unlock(&lock); 
    } 

    // server getting Shutdown 
    close(server_fd);
    pthread_mutex_destroy(&lock);
    pthread_mutex_destroy(&client_round_lock);
    pthread_cond_destroy(&round_cond);
    pthread_cond_destroy(&client_round_cond);
    pthread_cond_destroy(&game_start_cond);
    printf("Server got shut down.\n");
}

// Client Implementation 

void client(const char *host, int port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    struct hostent *server = gethostbyname(host);
    if (server == NULL) {
        fprintf(stderr, "ERROR, no such host: %s\n", host);
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    memcpy(&serv_addr.sin_addr.s_addr, server->h_addr_list[0], server->h_length);

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Failed to connect");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    printf("Connected to the server at %s:%d.\n\n", host, port);

    char buffer[BUFSIZE];
    int n;
    
    while (1) {
        memset(buffer, 0, BUFSIZE);
        n = recv(sockfd, buffer, BUFSIZE - 1, 0);
        
        if (n <= 0) {
            if (n == 0) {
                printf("Server closed the connection.\n");
            } else {
                perror("recv failed");
            }
            break;
        }
        
        buffer[n] = '\0';
        printf("%s", buffer);
        
        if (strstr(buffer, "Enter") || strstr(buffer, "Choice") || strstr(buffer, "Your move")) {
            char input[BUFSIZE];
            if (fgets(input, BUFSIZE, stdin) == NULL) {
                printf("\nInput error. Exiting.\n");
                break;
            }
            
            // Remove newline character if present
            size_t len = strlen(input);
            if (len > 0 && input[len-1] == '\n') {
                input[len-1] = '\0';
                len--;
            }
            `
            if (send(sockfd, input, len, 0) < 0) {
                perror("send failed");
                break;
            }
        }
    }
    
    close(sockfd);
}


// void client(char *server_node, int port) {
//     int fd = socket(AF_INET, SOCK_STREAM, 0);
//     if (fd < 0) { perror("socket failed"); exit(EXIT_FAILURE); }

//     struct hostent *server = gethostbyname(server_node);
//     if (server == NULL) {
//         fprintf(stderr, "ERROR, no such host: %s\n", server_node);
//         close(fd); exit(EXIT_FAILURE);
//     }

//     struct sockaddr_in addr;
//     addr.sin_family = AF_INET;
//     addr.sin_port = htons(port);
//     memcpy(&addr.sin_addr.s_addr, server->h_addr_list[0], server->h_length);

//     if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
//         perror("connect failed"); close(fd); exit(EXIT_FAILURE);
//     }

//     // get and send player name
//     printf("Enter your Move: ");
//     char name[NAMESIZE];
//     if (fgets(name, NAMESIZE, stdin) == NULL) {
//         fprintf(stderr, "Failed to get name.\n"); close(fd); exit(EXIT_FAILURE);
//     }
//     name[strcspn(name, "\n")] = 0;
//     if (send(fd, name, strlen(name), 0) < 0) {
//         perror("send name failed"); close(fd); exit(EXIT_FAILURE);
//     }

//     printf("Connected as the name %s. Waiting for server to give response...\n", name);

//     // main loop to receive messages and send input when prompted
//     char buffer[BUFSIZE];
//     int n;
//     while ((n = recv(fd, buffer, BUFSIZE - 1, 0)) > 0) {
//         buffer[n] = '\0';  
//         printf("%s", buffer); // printing server message

//         // check if the message contains the prompt for input
//         if (strstr(buffer, "Choose (R/P/S/L/K/Q/M/T):")) {
//             int ipt_sent = 0;
//             while (!ipt_sent) { // loop until valid input is sent for this prompt
//                 printf("Your choice: ");
//                 fflush(stdout); // ensurin prompt is displayed

//                 char input_buffer[BUFSIZE];
//                 if (fgets(input_buffer, BUFSIZE, stdin) == NULL) {
//                     printf("\nInput error/EOF. Sending 'Q' to quit.\n");
//                     if (send(fd, "Q", 1, 0) < 0) perror("send Q failed");
//                     ipt_sent = 1; 
//                     close(fd); exit(0); 
//                 }
//                 input_buffer[strcspn(input_buffer, "\n")] = 0; // remove newline

//                 if (strlen(input_buffer) >= 1) {
//                     char choice = toupper(input_buffer[0]);
//                     if (strchr("RPSLKQMT", choice)) { // check for valid command/move
//                         if (send(fd, &choice, 1, 0) < 0) {
//                             perror("send move failed"); close(fd); exit(1); 
//                         }
//                         ipt_sent = 1; // valid input sent and break inner loop
//                     } else {
//                         printf("Invalid input '%c'. Please choose R/P/S/L/K/Q/M/T.\n", input_buffer[0]);
//                     }
//                 } else {
//                     printf("No input has been entered. Please choose R/P/S/L/K/Q/M/T.\n");
//                 }
//             } 
//         } else if (strstr(buffer, "GAME OVER") || strstr(buffer, "Server full") ||
//                    strstr(buffer, "Server busy") || strstr(buffer, "Error starting game")) {
//              printf("\n---Game Ended ---\n");
//             break;
//         }
//     } // End main recv loop

//     if (n == 0) printf("\nServer is disconnected.\n");
//     else if (n < 0) perror("recv got failed");

//     close(fd);
//     printf("Connection is closed.\n");
// }


// Main Function 
// parsing command line arguments and starting either the server or client as per the arg
int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <-s|-c> <port> [hostname for -c]\n", argv[0]);
        exit(1);
    }

    int port = atoi(argv[2]);
    if (port <= 0 || port > 65535) {
        fprintf(stderr, "Invalid port number: %s\n", argv[2]);
        exit(1);
    }

    if (strcmp(argv[1], "-s") == 0) { // start the server
        if (argc != 3) {
            fprintf(stderr, "Usage: %s -s <port>\n", argv[0]); exit(1);
        }
        server(port);
    } else if (strcmp(argv[1], "-c") == 0) { // start the client
        char *hostname = "10.244.219.15"; // default hostname
        if (argc == 4) {
            hostname = argv[3]; // use provided hostname if given
        } else if (argc != 3) { // show error if args are wrong for client mode
             fprintf(stderr, "Usage: %s -c <port> [hostname]\n", argv[0]); exit(1);
        }
        client(hostname, port);
    } else { // invalid mode argument
        fprintf(stderr, "Usage: %s <-s|-c> <port> [hostname for -c]\n", argv[0]);
        exit(1);
    }

    return 0;
}