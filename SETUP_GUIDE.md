# Comprehensive Setup Guide for ETL-STOCKS-PIPELINE

## Git Workflow

1. **Cloning the Repository**
   - Open your terminal or command prompt.
   - Navigate to the directory where you want to clone the repository.
   - Execute the following command:
     ```bash
     git clone https://github.com/Chris03-be/ETL-STOCKS-PIPELINE.git
     ```

2. **Navigating the Repository**
   - Change into the repository's directory:
     ```bash
     cd ETL-STOCKS-PIPELINE
     ```

## Branching Strategy

- **Main Branch (`main`)**: This is the stable version of the code. It should always reflect a production-ready state.
- **Development Branches**: Use feature branches for new features. Name them descriptively. For example:
  - `feature/awesome-feature`
  - `bugfix/issue-123`

### Creating a New Branch
1. Ensure you are on the `main` branch:
   ```bash
   git checkout main
   ```
2. Pull the latest changes:
   ```bash
   git pull origin main
   ```
3. Create and switch to your new branch:
   ```bash
   git checkout -b feature/my-new-feature
   ```

## Best Practices for Working with VSCode

- **Install GitLens**: This VSCode extension supercharges the Git capabilities.
- **Using the Integrated Terminal**: Open the terminal in VSCode (`View > Terminal`) to run Git commands directly from your editor.
- **Source Control View**: Use the Source Control view (`View > Source Control`) to see changes, stage files, and create commits easily.

### Committing Changes
- Stage your changes:
  ```bash
  git add .
  ```
- Commit your changes with a descriptive message:
  ```bash
  git commit -m "Add awesome feature"
  ```

### Pushing Changes
- Push your changes to the remote repository:
  ```bash
  git push origin feature/my-new-feature
  ```

## Merging Changes
- Once your feature is ready, create a Pull Request on GitHub to merge changes into `main`.
- Make sure to review and resolve any merge conflicts if they arise. 

## Conclusion
Following this setup guide will help maintain a clean and organized workflow within the ETL-STOCKS-PIPELINE repository. Happy Coding!