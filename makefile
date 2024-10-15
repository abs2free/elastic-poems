dl:
	 git clone git@github.com:chinese-poetry/chinese-poetry.git --depth=1



import: build
	./poems -dir chinese-poetry/宋词
	./poems -dir chinese-poetry/全唐诗
	./poems -dir chinese-poetry/五代诗词


build:
	go build -o poems  .

