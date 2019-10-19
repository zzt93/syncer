
## Write a Issue

We can have a discussion about what to do and how to do
## Coding

Fix a bug or coding a feature

## Add Test Case

Write test cases for your commit: [examples](test/cases/)

- Add mysql table if necessary
    - add database & table definition for test data generation in test/generate/: name pattern `${db}.sql`
- Add config files in [config dir](test/config)
- Set up env 
- Generate test data
- Load data
- Compare source and sync target