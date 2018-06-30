library(scopeR)
filename <- 'credentials.txt'
cred <- data.frame(username = readline(prompt = "Username: "), 
                   password = .rs.askForPassword('Enter password'), 
                   stringsAsFactors = FALSE)
encrypt(cred, filename)
rm(cred, out)
# decrypt(filename)