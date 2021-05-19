FROM node:lts-alpine as build
WORKDIR /app
RUN mkdir /crawler
ENV PATH /app/node_modules/.bin:$PATH
COPY ./golemstats-frontend/package.json /app/package.json
RUN npm install --silent
RUN npm install -g @vue/cli
COPY ./golemstats-frontend/ /app
RUN npm run build

# production environment
FROM nginx:1.19.9-alpine
COPY --from=build /app/dist /usr/share/nginx/html
CMD ["nginx", "-g", "daemon off;"]