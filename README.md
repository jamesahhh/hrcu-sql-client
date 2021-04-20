# HRCU SQL Instructions

| Pre-Requisites |
| -------------- |
| Node 14.x.x +  |
| .env           |

:warning: You will need to get the environment variables from the creator or organization

1. Git clone or download the repository into the desired location
2. run command to install all relevant packages in the project directory

```js
npm install
```

3. To check if node windows is globally installed on your system

```js
npm list -g node-windows

//if not installed globally run the following

npm i -g node-windows
```

4. Link node-windows to project directory

```js
npm link node-windows
```

5. Run install script through npm

```js
npm run install

// if you wish to uninstall

npm run uninstall
```

:book: The Service should automatically start and re-start on rebooting of the machine
