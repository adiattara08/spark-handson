
if __name__ == '__main__':
    model = keras.models.load_model('/kaggle/input/learn/perceptron.h5')


    def get_feature_test(path):
        images = []  # Liste pour stocker les images

        id_img = []

        df = pd.read_csv(f'/kaggle/input/UBC-OCEAN/test.csv')

        df['file_path'] = df['image_id'].apply(lambda x: get_file_path_thumbnails(path, x))

        # Parcourir les lignes du dataframe
        for idx, row in df.iterrows():
            img_path = row['file_path']
            img_idx = row['image_id']

            # Vérifiez si img_path n'est pas nul avant de continuer
            if pd.notna(img_path):
                image = cv2.imread(img_path)
                # Assurez-vous que "chemin_image" n'est pas utilisé ici, utilisez plutôt "img_path"
                images.append(image)
                id_img.append(img_idx)

        return id_img, images


    def get_file_path_thumbnails(path_thumbnails, image_id):
        if os.path.exists(f"{path_thumbnails}/{image_id}_thumbnail.png"):
            return f"{path_thumbnails}/{image_id}_thumbnail.png"


    def get_file_path(path, image_id):
        if os.path.exists(f"{path}/{image_id}_thumbnail.png"):
            return f"{path}/{image_id}.png"


    def resize_img(image, img_size):
        resized_image = cv2.resize(image, (img_size, img_size))
        return resized_image


    test_df['file_path'] = test_df['image_id'].apply(lambda x: get_file_path_thumbnails(TEST_THUMBNAILS, x))
    tags = ['EC', 'MC', 'CC', 'LGSC', 'HGSC']
    id_img, x_test = get_feature_test(TEST_THUMBNAILS)

    resize_image_test = [resize_img(img, 256) for img in x_test]
    resize_image_test = np.array(resize_image_test)

    y_pred = model.predict(resize_image_test)

    predicted_labels = [tags[np.argmax(pred)] for pred in y_pred]

    # Print or further process the predicted labels
    print(predicted_labels)