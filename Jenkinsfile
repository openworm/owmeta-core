pipeline {
    agent any
    stages {
        stage('before install') {
            sh '''
            pip install --upgrade pip
            pip install coveralls
            if [ "$DEPLOY" ] ; then pip install twine ; fi
            pip uninstall pytest --yes
            pip install pytest>=3.4.0
            '''
        }
        stage('install') {
            sh './ci-install.sh'
        }
        stage('test') {
            sh './ci-test.sh'
        }
    }
}
