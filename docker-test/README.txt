To run tests in Docker:

1. Make sure Docker is installed

2. Build the docker image:

    $ ./build-image

3. Run the tests:

    $ ./run-tests

This will:
- Copy the code into the Docker machine
- Do a fresh install from NPM
- Run the tests
