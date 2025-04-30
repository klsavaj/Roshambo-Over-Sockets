# Spock Game

This repository contains the implementation of the Spock Game, where the game server acts as the central administrator and 5 clients (players) join the game. The game consists of 5 rounds, and after all rounds are complete, the winner is declared.

---

## Assumptions

- **Game Architecture:**  
  The game server works as the central administrator and 5 clients (players) join the game.

- **Player Participation:**  
  - All 5 players must join before the game starts.
  - After joining, a player can quit the game if desired.
  - After starting the game, players can quit but no other player can join the middle of the game.

- **Game Rounds:**  
  There are 5 rounds in the game. Once all rounds are complete, the winner is declared.
---

## How to Run the Code

### Setting Up the Server

1. **Open a Terminal.**
2. **Clone the Repository.**
3. **Navigate to the `HW3` Folder:**
   ```bash
   cd HW3
   ```
4. **Run the makefile**
   ```bash
   make clean && make
   ```
5. **Run server mode (Note: You can select any port(free) as per your convenience) 	This terminal will act as the server.)**
   ```bash
   ./spock -s 51515
   ```
### Setting Up the Client 
1. **Open 5 terminals**
2. **Run client mode**
   ```bash
   ./spock -c 51515
   ```
   - Note: once you open the 5 terminals and write above command, then game will start and you will be shown the choices(R/P/S/L/K/M/T/Q) in the game.
### Enjoy Playing the game
