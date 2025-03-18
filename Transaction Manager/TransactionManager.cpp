#include <iostream>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <stdexcept>
#include <stack>
#include <string>
#include <direct.h> 

using namespace std;

class TransactionManager {
public:

    void loadFileData(const string& filename) {
        ifstream file(filename.c_str()); 
        if (!file) {
            
            fileData_[filename] = vector<string>();
            cout << "File '" << filename << "' does not exist. A new file will be created on commit." << endl;
            return;
        }

        vector<string> lines;
        string line;
        while (getline(file, line)) {
            lines.push_back(line);
        }
        fileData_[filename] = lines;
    }

    void addInsertOperation(const string& filename, const string(&data)[10]) {
 
        for (int i = 0; i < 10; i++) {  
            if (!data[i].empty() && isDataAlreadyInFile(filename, data[i])) {  
                cout << "Data already exists in file: " << filename << endl;
                return;  
            }
        }
    
        for (int i = 0; i < 10; i++) {  
            if (!data[i].empty()) {  
                fileData_[filename].push_back(data[i]);
            }
        }
    
        cout << "Data inserted successfully.\n";
    }
    

    void createSavepoint(const string& savepointName) {
        savepoints_[savepointName] = fileData_; 
        cout << "Savepoint '" << savepointName << "' created." << endl;
    }

    void rollbackToSavepoint(const string& savepointName) {
        if (savepoints_.find(savepointName) == savepoints_.end()) {
            throw runtime_error("Savepoint '" + savepointName + "' not found.");
        }

        fileData_ = savepoints_[savepointName]; 
        cout << "Rolled back to savepoint '" << savepointName << "'." << endl;
        commit();
    }

    void commit() {
        cout << "Committing changes..." << endl;
        for (unordered_map<string, vector<string>>::const_iterator it = fileData_.begin(); it != fileData_.end(); ++it) {
            const string& filename = it->first;
            const vector<string>& lines = it->second;

            try {
                ofstream file(filename.c_str()); 
                if (!file) {
                    throw runtime_error("Failed to open file for committing: " + filename);
                }
                for (vector<string>::const_iterator lineIt = lines.begin(); lineIt != lines.end(); ++lineIt) {
                    file << *lineIt << endl; 
                }
                cout << "Data written to file: " << filename << endl;
            } catch (const exception& e) {
                cerr << "Error writing to file '" << filename << "': " << e.what() << endl;
            }
        }
        savepoints_.clear(); 
    }

    void rollback() {
        cout << "Rolling back changes..." << endl;
        fileData_.clear();
        savepoints_.clear();
    }

    bool isDataAlreadyInFile(const string& filename, const string& data) {
        if (fileData_.find(filename) == fileData_.end()) {
            return false; 
        }

        const vector<string>& lines = fileData_[filename];
        for (vector<string>::const_iterator it = lines.begin(); it != lines.end(); ++it) {
            if (*it == data) {
                return true; 
            }
        }
        return false; 
    }

private:
    unordered_map<string, vector<string>> fileData_; 
    unordered_map<string, unordered_map<string, vector<string>>> savepoints_;
};

int main() {
    TransactionManager tm;
    string filename, data, savepointName;
    int choice;

    cout << "Enter file name to load: ";
    cin >> filename;
    tm.loadFileData(filename);

    while (true) {
        system("cls");
        string input;
        string data[10];
        cout << "\n1. Insert Data\n2. Create Savepoint\n3. Rollback to Savepoint\n4. Commit\n5. Rollback\n6. Exit\n";
        cout << "Enter your choice: ";
        cin >> choice;

            if (choice==1)
            {
                int i=0;
                while(i<10)
                {
                i++;
                cout << "Enter data to insert and e to exit: ";
                getline(cin, input);
                if(input=="e")
                {
                    break;
                }
                data[i]=input;
                input="";
                }
                tm.addInsertOperation(filename, data);
                
            }
            if (choice==2)
            {
                cout << "Enter savepoint name: ";
                cin >> savepointName;
                tm.createSavepoint(savepointName);
                
            }
            if(choice==3)
            {
                cout << "Enter savepoint name to rollback to: ";
                cin >> savepointName;
                tm.rollbackToSavepoint(savepointName);
                
            }
            if(choice==4)
            {
                tm.commit();
                cout << "Transaction committed successfully." << endl;
            
            }
            if(choice==5)
            {
                tm.rollback();
                cout << "Transaction rolled back." << endl;
                
            }
            if(choice==6)
            {
                cout << "Exiting..." << endl;
                return 0;
            }
            else
            {
                cout << "Invalid choice. Try again." << endl;
            }
        }


    return 0;
}
