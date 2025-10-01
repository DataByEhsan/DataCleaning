<p align="center">
  <img src="https://img.shields.io/badge/Data%20Cleaning-Professional-blue?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Status-Active-success?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Made%20With-SQL%20%26%20Python-orange?style=for-the-badge" />
</p>

<h1 align="center">🧹 Data Cleaning & Preprocessing Repository</h1>
<p align="center">Transforming raw, messy datasets into structured, analytics-ready data pipelines.</p>

---

## 📚 Table of Contents
- [Purpose](#-purpose)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Cleaning Techniques Used](#-cleaning-techniques-used)
- [Before & After Example](#-before--after-example)
- [Contribution](#-contribution)
- [License](#-license)

---

## 🎯 Purpose

This repository serves as a central hub for reusable **data cleaning workflows**, handling challenges such as:

✔ Replacing invalid or inconsistent values (`ERROR`, `UNKNOWN`, `NULL`, etc.)  
✔ Converting data types (string → float → datetime)  
✔ Normalizing categories with controlled mappings  
✔ Recalculating missing numerical values when possible  
✔ Exporting clean, standardized datasets ready for analytics & ML pipelines

---

## 🛠 Tech Stack

| Technology | Use Case |
|------------|-----------------------------|
| **SQL** | Bulk transformations & structured cleaning |
| **Excel / CSV Review** | Manual inspection & validation |

---

## 📂 Project Structure
/dataset-name/
│── raw/ # Original dataset
│── cleaned/ # Final output
│── sql/ # SQL cleaning scripts
│── docs.md # Notes & summary of transformations


---

## 🧼 Cleaning Techniques Used

- Null replacement via `COALESCE`, `NULLIF`, `CASE`
- Type casting & schema enforcement
- Conditional imputation (e.g. `price = total / quantity`)
- Lookup-based category replacement
- Standardized fallback rules (`default price`, `fallback date`, etc.)

---

## 🔄 Before / After Example

| Transaction_ID | Item     | Quantity | Total_Spent | → | Cleaned_Item | Final_Quantity | Final_Total |
|----------------|---------|----------|-------------|---|--------------|----------------|-------------|
| 101            | UNKNOWN | ERROR    | 6.0         | → | Coffee       | 3              | 6.0         |
| 102            | Tea     | 2        | UNKNOWN     | → | Tea          | 2              | 3.0         |

---

## 🤝 Contribution

Got a messy dataset you'd like to clean or standardize?  
Feel free to **open an Issue** or **submit a Pull Request.**

---

## 📄 License

This repository is licensed under the **MIT License** — free to use and extend.

---

<p align="center"><b>From raw to reliable — one dataset at a time.</b></p>

