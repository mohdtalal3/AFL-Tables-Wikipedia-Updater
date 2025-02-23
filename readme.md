# 🏉 AFL Tables Scraper & Wikipedia Updater  

This **open-source** project scrapes real-time AFL data from [AFL Tables](https://afltables.com/) and updates relevant Wikipedia pages automatically.  

## ✨ Features  
✔️ Scrapes **AFL data in real-time** from AFL Tables.  
✔️ Updates Wikipedia pages with **up-to-date statistics**.  
✔️ User-configurable **scraping frequency, year, and threading**.  
✔️ Secure Wikipedia **authentication using environment variables**.  
✔️ **Completely free and open-source** for everyone!  

---

## 🛠️ Prerequisites  
- **Python 3.x** installed on your system.  
- **Wikipedia account credentials** stored in an `.env` file.  

---

## 🚀 Installation & Setup  

### **1️⃣ Install Python**  
#### 🔹 Windows & Mac  
- Download Python from the official site: [Python Downloads](https://www.python.org/downloads/)  
- Install and verify using:  
  ```sh
  python --version
  ```

## 🔽 Cloning the Project  

To clone this repository to your local machine, open **Terminal / Command Prompt** and run the following command:  

```sh
git clone https://github.com/mohdtalal3/AFL-Tables-Wikipedia-Updater.git
```
## 📂 Navigate into the Project Directory
```sh
cd AFL-Tables-Wikipedia-Updater
```

### 2️⃣ Install Required Packages  
- Open **Terminal / Command Prompt**  
- Navigate to your project directory  
- Install dependencies using:  

  ```sh
  pip install -r requirements.txt
  ```

### 3️⃣ Configure Environment Variables  
- Create a `.env` file in the project directory.  
- Add your Wikipedia username:  

  ```ini
  WIKI_USERNAME=your_username
  ```
When running the script for the first time, you’ll be prompted to enter your Wikipedia password.
(It will be hidden for security reasons.)

## ▶️ Running the Scraper  
- Start the program by running:  

  ```sh
  python afl_scraper.py
  ```
The script will prompt you to enter:  
- 📅 **Scraping frequency** (in days)  
- 📆 **Year to scrape** (between 1897 and the current year)  
- ⚡ **Number of threads** (1-20)  


## ✅ Example Input & Execution  

  ```sh
  Enter how often to run the scraper (in days): 2  
  Enter the year to scrape (e.g., 2024): 2024  
  Enter the number of threads to use (1-20): 5  
  Enter your Wikipedia password: (hidden input)  
  ```
## 📌 Important Notes  

⚠️ The **terminal must remain open** while the program is running. Closing it will stop the process.  

⚠️ Ensure that **you have permission** to edit Wikipedia pages before running the script.  

## 🏆 Credits  
- 👨‍💻 **Code by:** Muhammad Talal  
- 💰 **Funded by:** Ben Schultz  
- 📜 **Permission to Scrape Granted by:** AFL Tables Admins  

---

## 📜 License  
This project is **open-source** and available for anyone to use and contribute to!  
