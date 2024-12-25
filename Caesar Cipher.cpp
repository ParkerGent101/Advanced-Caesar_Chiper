#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <cctype>
#include <sstream> 

std::string enhancedCaesarCipher(const std::string& text, int shift, bool encrypt) {
    std::string result = ""; //  return string
    int direction = (encrypt) ? 1 : -1; // wether to encrypt (1) or decrypt (-1)

    // creating the plaintext / ciphertext character by character
    // moves the char num by shift using modulo 26 + original char
    for (char c : text) {
        if (std::isalpha(c)) {
            char base = (std::isupper(c)) ? 'A' : 'a'; //lower or uppercase char
            c = (c - base + direction * shift + 26) % 26 + base; // offset character code + encrypt or decrypt + 26 modulo returns character positions 0 ot 25 + base value 
        }
        result += c; //build response
    }

    return result;
}

std::vector<std::string> readVocabulary(const std::string& filename) {
    std::vector<std::string> words; // holds possible words
    std::ifstream file(filename); // input file
    std::string word; // current Word

    // reading through file
    while (file >> word) {
        words.push_back(word);
    }

    return words; // all possible plaintext words
}

bool isValidDecryption(const std::string& decryptedText, const std::vector<std::string>& vocabulary) {
    std::istringstream iss(decryptedText); // create string stream to tokenize decrypted text
    std::string token;

    // iterate through each word in possibly decrypted text
    while (iss >> token) {
        // Remove punctuation from the token
        token.erase(std::remove_if(token.begin(), token.end(), ::ispunct), token.end());
        
        bool found = false;

        // iterate through each word in .txt bank vocabulary
        for (const std::string& vocabWord : vocabulary) {

            std::string vocabWordWithoutPunctuation = vocabWord; // .txt word holder without punctuation
            vocabWordWithoutPunctuation.erase(std::remove_if(vocabWordWithoutPunctuation.begin(), vocabWordWithoutPunctuation.end(), ::ispunct), vocabWordWithoutPunctuation.end()); // remove punctuation from word for comparison

            // compare the token without puncutation with current word
            if (token == vocabWordWithoutPunctuation) {
                found = true; // marks token as found in vocab
                break; // exit loop since a match was found in .txt file
            }
        }

        // if the token is not found in .txt 
        if (!found) {
            return false;
        }
    }
    return true;
}

int main() {
    char choice;
    std::string input;

    // running program
    while (true) {
        std::cout << "Encrypt or decrypt (e/d) or perform brute-force attack (b), q to quit: "; // user choses function
        std::cin >> choice;

        // quit Program
        if (choice == 'q') {
            std::cout << "Exiting." << std::endl;
            break;
        }
        
        // error checking input
        if (choice != 'e' && choice != 'd' && choice != 'b') {
            std::cout << "Invalid choice." << std::endl;
            continue;
        }

        std::cin.ignore(); // Clear the newline from the input buffer

        // encrypt or decrypt casear cipher
        if (choice == 'e' || choice == 'd') {
            std::cout << "Enter a string of letters and spaces: ";
            std::getline(std::cin, input);

            int number;
            std::string outputText;

            // encrypt a message
            if (choice == 'e') {
                std::cout << "Enter a key (positive integer): ";
                std::cin >> number;
                outputText = enhancedCaesarCipher(input, number, true);
                std::cout << "Encrypted text: " << outputText << std::endl;
            } 
            
            // decrypt a message
            else {
                std::cout << "Enter the key used for encryption: ";
                std::cin >> number;
                outputText = enhancedCaesarCipher(input, number, false);
                std::cout << "Decrypted text: " << outputText << std::endl;
            }

        // brute force
        } else if (choice == 'b') {
            std::string inputFilename;
            std::string ciphertext;
            std::vector<std::string> vocabulary;

            std::cout << "Enter the ciphertext message: ";
            std::getline(std::cin, ciphertext);

            std::cout << "Enter the vocabulary text file name: ";
            std::cin >> inputFilename;

            vocabulary = readVocabulary(inputFilename);

            bool found = false;
            int key;

            // searches all possible keys
            for (key = 1; key <= 26; ++key) {
                std::string decryptedText = enhancedCaesarCipher(ciphertext, key, false); // Tries to decrypt using a key
                if (isValidDecryption(decryptedText, vocabulary)) {
                    found = true; // returns true if the isValidDecryption finds a word found in input .txt bank of words
                    break;
                }
            }

            // outputs the message if found
            if (found) {
                std::cout << "Key: " << key << std::endl;
                std::cout << "Deciphered plaintext message: " << enhancedCaesarCipher(ciphertext, key, false) << std::endl;
            } 
            
            // if the words are not found in the .txt bank
            else {
                std::cout << "Decryption unsuccessful." << std::endl;
            }
        }
    }

    return 0;
}
