pipeline {
    agent any
    stages {
        stage('before install') {
            steps {
                sh '''
                pip install --upgrade pip
                pip install coveralls
                if [ "$DEPLOY" ] ; then pip install twine ; fi
                pip uninstall pytest --yes
                pip install pytest>=3.4.0
                '''
            }
        }
        stage('install') {
            steps {
                sh './ci-install.sh'
            }
        }
        stage('test') {
            steps {
                sh './ci-test.sh'
            }
        }
    }
}
