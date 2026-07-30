# Build stage
FROM node:22-alpine AS build
WORKDIR /app

# Copy package descriptors and install packages
COPY package*.json ./
RUN npm ci

# Copy sources and build
COPY . .
RUN npm run build -- --configuration=production

# Run stage
FROM nginx:alpine
# Copy built static files to Nginx directory
COPY --from=build /app/dist/logistics-frontend/browser /usr/share/nginx/html

# Copy simple Nginx config to handle Angular routing (fallback to index.html)
RUN echo 'server { \
    listen 80; \
    location / { \
        root /usr/share/nginx/html; \
        index index.html index.htm; \
        try_files $uri $uri/ /index.html; \
    } \
}' > /etc/nginx/conf.d/default.conf

EXPOSE 80
ENTRYPOINT ["nginx", "-g", "daemon off;"]
